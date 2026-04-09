package com.jmqx.broker;

import com.jmqx.acl.AclAction;
import com.jmqx.acl.AclAuthorizer;
import com.jmqx.acl.AclRequest;
import com.jmqx.bridge.BridgeMessage;
import com.jmqx.bridge.MessageBridge;
import com.jmqx.common.SharedSubscription;
import com.jmqx.protocol.BrokerMessageHandler;
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.router.SharedSubscriptionManager;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.router.SubscriptionMatchResult;
import com.jmqx.router.global.GlobalSubscriptionEvent;
import com.jmqx.router.global.GlobalSubscriptionMatch;
import com.jmqx.router.global.GlobalSubscriptionRegistry;
import com.jmqx.session.ClientSession;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.RetainedMessage;
import com.jmqx.store.RetainedMessageStore;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.util.AttributeKey;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT 核心消息处理器。
 * 负责连接鉴权、会话建立、订阅管理、消息路由和 ACL 校验。
 *
 * @author liucaiwen
 * @date 2026/4/2
 */
public class MqttBrokerMessageHandler implements BrokerMessageHandler {
    private static final Logger LOG = Logger.getLogger(MqttBrokerMessageHandler.class.getName());
    private static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("jmqx.clientId");
    private static final AttributeKey<Boolean> CLEAN_SESSION = AttributeKey.valueOf("jmqx.cleanSession");
    private static final AttributeKey<String> WS_USERNAME = AttributeKey.valueOf("jmqx.ws.username");
    private static final AttributeKey<String> CONNECTION_TYPE = AttributeKey.valueOf("jmqx.connectionType");
    private static final AttributeKey<Boolean> GRACEFUL_DISCONNECT = AttributeKey.valueOf("jmqx.gracefulDisconnect");
    private static final AttributeKey<WillMessage> WILL_MESSAGE = AttributeKey.valueOf("jmqx.willMessage");

    private static final String TOPIC_CLIENT_CONNECTED = "$SYS/jmqx/events/client/connected";
    private static final String TOPIC_CLIENT_DISCONNECTED = "$SYS/jmqx/events/client/disconnected";

    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private final RetainedMessageStore retainedMessageStore;
    private final ClientAuthenticator clientAuthenticator;
    private final AclAuthorizer aclAuthorizer;
    private final SharedSubscriptionManager sharedSubscriptionManager;
    private final MessageBridge messageBridge;
    private final boolean retainedEnabled;
    private final ExecutorService retainedStoreExecutor;
    private final GlobalSubscriptionRegistry globalSubscriptionRegistry;
    private final String nodeId;
    private final ClusterMessageDispatcher clusterMessageDispatcher;
    private final AtomicLong globalRouteLogIndex = new AtomicLong(0);
    private final ConcurrentMap<String, AtomicLong> sharedGroupNodeRoundRobin = new ConcurrentHashMap<>();

    public MqttBrokerMessageHandler(
            SessionRegistry sessionRegistry,
            SubscriptionRegistry subscriptionRegistry,
            RetainedMessageStore retainedMessageStore,
            ClientAuthenticator clientAuthenticator,
            AclAuthorizer aclAuthorizer,
            SharedSubscriptionManager sharedSubscriptionManager,
            MessageBridge messageBridge,
            boolean retainedEnabled,
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            String nodeId) {
        this(
            sessionRegistry,
            subscriptionRegistry,
            retainedMessageStore,
            clientAuthenticator,
            aclAuthorizer,
            sharedSubscriptionManager,
            messageBridge,
            retainedEnabled,
            globalSubscriptionRegistry,
            nodeId,
            ClusterMessageDispatcher.NOOP
        );
    }

    public MqttBrokerMessageHandler(
            SessionRegistry sessionRegistry,
            SubscriptionRegistry subscriptionRegistry,
            RetainedMessageStore retainedMessageStore,
            ClientAuthenticator clientAuthenticator,
            AclAuthorizer aclAuthorizer,
            SharedSubscriptionManager sharedSubscriptionManager,
            MessageBridge messageBridge,
            boolean retainedEnabled,
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            String nodeId,
            ClusterMessageDispatcher clusterMessageDispatcher) {
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.retainedMessageStore = retainedMessageStore;
        this.clientAuthenticator = clientAuthenticator;
        this.aclAuthorizer = aclAuthorizer;
        this.sharedSubscriptionManager = sharedSubscriptionManager == null
            ? new SharedSubscriptionManager()
            : sharedSubscriptionManager;
        this.messageBridge = messageBridge == null ? MessageBridge.NOOP : messageBridge;
        this.retainedEnabled = retainedEnabled;
        this.globalSubscriptionRegistry = globalSubscriptionRegistry;
        this.nodeId = (nodeId == null || nodeId.isBlank()) ? "node-1" : nodeId;
        this.clusterMessageDispatcher = clusterMessageDispatcher == null
            ? ClusterMessageDispatcher.NOOP
            : clusterMessageDispatcher;
        this.retainedStoreExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "jmqx-retained-store");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void onMessage(ChannelHandlerContext ctx, MqttMessage message) {
        // 协议解析失败，直接关闭连接
        if (message.decoderResult().isFailure()) {
            LOG.warning(() -> "[PROTO] decode failed, remote=" + ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // MQTT 控制报文统一在这里分发，底层 transport 只负责把报文安全地交上来。
        MqttMessageType messageType = message.fixedHeader().messageType();
        switch (messageType) {
            case CONNECT -> handleConnect(ctx, (MqttConnectMessage) message);
            case SUBSCRIBE -> handleSubscribe(ctx, (MqttSubscribeMessage) message);
            case UNSUBSCRIBE -> handleUnsubscribe(ctx, (MqttUnsubscribeMessage) message);
            case PUBLISH -> handlePublish(ctx, (MqttPublishMessage) message);
            case PINGREQ -> {
                LOG.fine(() -> "[PING] clientId=" + currentClientId(ctx.channel()));
                ctx.writeAndFlush(new MqttMessage(
                        new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)
                ));
            }
            case DISCONNECT -> {
                ctx.channel().attr(GRACEFUL_DISCONNECT).set(true);
                LOG.fine(() -> "[DISCONNECT] clientId=" + currentClientId(ctx.channel()));
                ctx.close();
            }
            default -> {
            }
        }
    }

    @Override
    public void onDisconnect(Channel channel) {
        String clientId = channel.attr(CLIENT_ID).get();
        if (clientId == null || clientId.isBlank()) {
            return;
        }

        ClientSession session = sessionRegistry.get(clientId).orElse(null);
        String username = session == null ? null : session.username();
        String connectionType = session == null ? null : session.connectionType();
        String serviceNodeIp = session == null ? null : session.serviceNodeIp();
        int keepAliveSeconds = session == null ? 0 : session.keepAliveSeconds();
        // cleanSession 断开时直接清理订阅，持久会话则只移除在线 channel。
        boolean cleanSession = Optional.ofNullable(channel.attr(CLEAN_SESSION).get()).orElse(false);
        boolean gracefulDisconnect = Optional.ofNullable(channel.attr(GRACEFUL_DISCONNECT).get()).orElse(false);

        // 仅在非正常断开时发送遗嘱消息，符合 MQTT 遗嘱语义。
        if (!gracefulDisconnect) {
            publishWillMessageIfPresent(channel, clientId);
        }

        sessionRegistry.remove(clientId);
        if (cleanSession) {
            sharedSubscriptionManager.removeClient(clientId);
            Set<String> removedLastTopics = subscriptionRegistry.removeClientAndCollectLastTopics(clientId);
            removedLastTopics.forEach(this::applyGlobalUnregisterAfterLocal);
        }
        publishClientLifecycleEvent(
            TOPIC_CLIENT_DISCONNECTED,
            clientId,
            username,
            connectionType,
            serviceNodeIp,
            keepAliveSeconds,
            gracefulDisconnect ? "graceful" : "unexpected"
        );
        LOG.fine(() -> "[SESSION] offline clientId=" + clientId + ", cleanSession=" + cleanSession);
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {
        String clientId = message.payload().clientIdentifier();
        if (clientId == null || clientId.isBlank()) {
            LOG.warning(() -> "[CONNECT] rejected empty clientId, remote=" + ctx.channel().remoteAddress());
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
            return;
        }

        String username = normalize(message.payload().userName());
        if (username == null) {
            username = normalize(ctx.channel().attr(WS_USERNAME).get());
        }
        if (username == null) {
            username = clientId;
        }
        String connectionType = normalize(ctx.channel().attr(CONNECTION_TYPE).get());
        if (connectionType == null) {
            connectionType = "mqtt";
        }
        String password = message.payload().passwordInBytes() == null
                ? null
                : new String(message.payload().passwordInBytes(), StandardCharsets.UTF_8);

        // 连接鉴权统一收敛在这里，WebSocket 握手阶段传过来的用户名也在这里一并复用。
        if (!clientAuthenticator.authenticate(clientId, username, password)) {
            LOG.warning("[CONNECT] auth failed clientId=" + clientId + ", username=" + username);
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
            return;
        }

        boolean cleanSession = message.variableHeader().isCleanSession();
        int keepAliveSeconds = Math.max(message.variableHeader().keepAliveTimeSeconds(), 0);
        String serviceNodeIp = resolveServiceNodeIp(ctx.channel());
        sessionRegistry.register(new ClientSession(
            clientId,
            ctx.channel(),
            connectionType,
            cleanSession,
            username,
            serviceNodeIp,
            keepAliveSeconds,
            Instant.now()
        ));
        ctx.channel().attr(CLIENT_ID).set(clientId);
        ctx.channel().attr(CLEAN_SESSION).set(cleanSession);
        ctx.channel().attr(GRACEFUL_DISCONNECT).set(false);

        WillMessage willMessage = buildWillMessage(message);
        if (willMessage != null) {
            ctx.channel().attr(WILL_MESSAGE).set(willMessage);
        } else {
            ctx.channel().attr(WILL_MESSAGE).set(null);
        }

        ctx.writeAndFlush(MqttMessageBuilders.connAck()
                .sessionPresent(false)
                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .build());
        publishClientLifecycleEvent(
            TOPIC_CLIENT_CONNECTED,
            clientId,
            username,
            connectionType,
            serviceNodeIp,
            keepAliveSeconds,
            "connected"
        );
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("[CONNECT] accepted clientId=" + clientId
                + ", connectionType=" + connectionType
                + ", username=" + username
                + ", serviceNodeIp=" + serviceNodeIp
                + ", cleanSession=" + cleanSession
                + ", keepAliveSeconds=" + keepAliveSeconds);
        }
    }

    /**
     * handle subscribe message
     *
     * @param ctx     ctx
     * @param message message
     */
    private void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null) {
            ctx.close();
            return;
        }

        List<Integer> grantedQos = new ArrayList<>();
        for (MqttTopicSubscription subscription : message.payload().topicSubscriptions()) {
            String topicFilter = subscription.topicFilter();
            String normalizedFilter = SharedSubscription.normalizeTopicFilter(topicFilter);
            if (allowed(clientId, normalizedFilter, AclAction.SUBSCRIBE)) {
                grantedQos.add(MqttQoS.FAILURE.value());
                LOG.warning(() -> "[ACL] subscribe denied clientId=" + clientId + ", topicFilter=" + topicFilter);
                continue;
            }
            // 目前只支持 QoS 0 和 1
            int qos = Math.min(subscription.qualityOfService().value(), 1);
            SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
            if (shared != null && !sharedSubscriptionManager.register(shared.group(), clientId)) {
                grantedQos.add(MqttQoS.FAILURE.value());
                LOG.warning(() -> "[SHARED] subscribe rejected by group limit clientId=" + clientId
                    + ", group=" + shared.group() + ", topicFilter=" + topicFilter);
                continue;
            }
            // 先更新本地主题数，再根据首个订阅结果更新全局路由表。
            boolean firstLocal = subscriptionRegistry.subscribeAndCheckFirst(clientId, topicFilter, qos);
            if (firstLocal) {
                applyGlobalRegisterAfterLocal(topicFilter);
            }
            grantedQos.add(qos);
            // MQTT 规范要求新订阅完成后立即回放匹配的 retained 消息。
            if (retainedEnabled) {
                replayRetained(ctx.channel(), normalizedFilter);
            }
            LOG.info(() -> "[SUBSCRIBE] clientId=" + clientId + ", topicFilter=" + topicFilter + ", qos=" + qos);
        }

        MqttMessageBuilders.SubAckBuilder subAckBuilder = MqttMessageBuilders.subAck()
                .packetId(message.variableHeader().messageId());

        grantedQos.forEach(qos -> {
            MqttQoS mqttQoS = MqttQoS.valueOf(qos);
            subAckBuilder.addGrantedQos(mqttQoS);
        });
        ctx.writeAndFlush(subAckBuilder.build());
    }

    private void handleUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null) {
            ctx.close();
            return;
        }

        message.payload().topics().forEach(topicFilter -> {
            SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
            if (shared != null) {
                sharedSubscriptionManager.unregister(shared.group(), clientId);
            }
            // 先更新本地主题数，再根据最后一个订阅结果更新全局路由表。
            boolean lastLocal = subscriptionRegistry.unsubscribeAndCheckLast(clientId, topicFilter);
            if (lastLocal) {
                applyGlobalUnregisterAfterLocal(topicFilter);
            }
        });
        ctx.writeAndFlush(MqttMessageBuilders.unsubAck().packetId(message.variableHeader().messageId()).build());
        LOG.fine(() -> "[UNSUBSCRIBE] clientId=" + clientId + ", topics=" + message.payload().topics());
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        byte[] payload = ByteBufUtil.getBytes(message.payload());
        int qos = message.fixedHeader().qosLevel().value();
        String clientId = currentClientId(ctx.channel());

        // ACL 鉴权
        if (allowed(clientId, topic, AclAction.PUBLISH)) {
            LOG.warning(() -> "[ACL] publish denied clientId=" + clientId + ", topic=" + topic);
            if (qos == 1) {
                ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(message.variableHeader().packetId()).build());
            }
            return;
        }

        if (message.fixedHeader().isRetain()) {
            // 先判断系统是否启用 retained，再异步写入/删除 retained 存储。
            submitRetainedAsync(topic, payload, qos);
        }

        // 单机模式下先完成本地投递，再写桥接通道。
        routeMessage(topic, payload);

        // 消息桥接到对应消息队列
        messageBridge.publish(new BridgeMessage(
            clientId,
            topic,
            payload,
            qos,
            message.fixedHeader().isRetain(),
            System.currentTimeMillis()
        ));
        LOG.fine(() -> "[PUBLISH] clientId=" + clientId + ", topic=" + topic
                + ", qos=" + qos + ", retain=" + message.fixedHeader().isRetain() + ", bytes=" + payload.length);

        if (qos == 1) {
            ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(message.variableHeader().packetId()).build());
        }
    }

    private WillMessage buildWillMessage(MqttConnectMessage connectMessage) {
        if (!connectMessage.variableHeader().isWillFlag()) {
            return null;
        }
        String topic = connectMessage.payload().willTopic();
        if (topic == null || topic.isBlank()) {
            return null;
        }
        byte[] payload = connectMessage.payload().willMessageInBytes();
        if (payload == null) {
            payload = new byte[0];
        }
        int qos = Math.max(connectMessage.variableHeader().willQos(), 0);
        boolean retain = connectMessage.variableHeader().isWillRetain();
        return new WillMessage(topic, payload.clone(), qos, retain);
    }

    private void publishWillMessageIfPresent(Channel channel, String clientId) {
        WillMessage willMessage = channel.attr(WILL_MESSAGE).get();
        if (willMessage == null) {
            return;
        }
        publishInternal(
            clientId,
            willMessage.topic(),
            willMessage.payload(),
            willMessage.qos(),
            willMessage.retain()
        );
        LOG.fine(() -> "[WILL] published clientId=" + clientId + ", topic=" + willMessage.topic());
    }

    private void publishClientLifecycleEvent(
            String topic,
            String clientId,
            String username,
            String connectionType,
            String serviceNodeIp,
            int keepAliveSeconds,
            String eventType) {
        String payload = "{\"event\":\"" + safeJson(eventType)
            + "\",\"clientId\":\"" + safeJson(clientId)
            + "\",\"username\":\"" + safeJson(username)
            + "\",\"connectionType\":\"" + safeJson(connectionType)
            + "\",\"serviceNodeIp\":\"" + safeJson(serviceNodeIp)
            + "\",\"keepAliveSeconds\":" + keepAliveSeconds
            + ",\"timestamp\":" + System.currentTimeMillis()
            + "}";
        publishInternal(
            "system",
            topic,
            payload.getBytes(StandardCharsets.UTF_8),
            MqttQoS.AT_MOST_ONCE.value(),
            false
        );
    }

    private void publishInternal(String sourceClientId, String topic, byte[] payload, int qos, boolean retain) {
        if (retain) {
            // 内部发布同样遵循 retained 开关，并使用异步持久化。
            submitRetainedAsync(topic, payload, qos);
        }
        routeMessage(topic, payload);
        messageBridge.publish(new BridgeMessage(
            sourceClientId,
            topic,
            payload,
            qos,
            retain,
            System.currentTimeMillis()
        ));
    }

    private void routeMessage(String topic, byte[] payload) {
        SubscriptionMatchResult matchResult = subscriptionRegistry.findSubscriptionMatch(topic);
        Set<String> subscribers = new LinkedHashSet<>(matchResult.getDirectSubscribers());
        matchResult.getSharedSubscribersByGroup().forEach((group, candidates) -> {
            String selected = sharedSubscriptionManager.selectSubscriber(group, candidates, sessionRegistry);
            if (selected != null) {
                subscribers.add(selected);
            }
        });
        LOG.fine(() -> "[ROUTE][LOCAL] topic=" + topic + ", subscribers=" + subscribers.size());
        for (String subscriber : subscribers) {
            Optional<ClientSession> sessionOptional = sessionRegistry.get(subscriber);
            if (sessionOptional.isEmpty()) {
                continue;
            }

            Channel channel = sessionOptional.get().channel();
            if (!channel.isActive()) {
                continue;
            }

            channel.writeAndFlush(MqttMessageBuilders.publish()
                    .topicName(topic)
                    .retained(false)
                    .qos(MqttQoS.AT_MOST_ONCE)
                    .payload(Unpooled.wrappedBuffer(payload))
                    .build());
        }

        Set<String> remoteNodeIds = collectRemoteTargetNodes(topic);
        if (!remoteNodeIds.isEmpty()) {
            clusterMessageDispatcher.dispatch(topic, payload.clone(), remoteNodeIds);
            LOG.fine(() -> "[ROUTE][CLUSTER] topic=" + topic + ", remoteNodes=" + remoteNodeIds);
        }
    }

    private Set<String> collectRemoteTargetNodes(String topic) {
        if (globalSubscriptionRegistry == null) {
            return Collections.emptySet();
        }
        GlobalSubscriptionMatch globalMatch = globalSubscriptionRegistry.match(topic);
        Set<String> targetNodes = new LinkedHashSet<>(globalMatch.getNormalNodes());
        for (Map.Entry<String, Set<String>> entry : globalMatch.getSharedGroupToNodes().entrySet()) {
            String selectedNode = selectSharedTargetNode(entry.getKey(), entry.getValue());
            if (selectedNode != null) {
                targetNodes.add(selectedNode);
            }
        }
        targetNodes.remove(nodeId);
        return targetNodes;
    }

    private String selectSharedTargetNode(String group, Set<String> nodes) {
        if (group == null || group.isBlank() || nodes == null || nodes.isEmpty()) {
            return null;
        }
        List<String> sortedNodes = new ArrayList<>(nodes);
        Collections.sort(sortedNodes);
        AtomicLong cursor = sharedGroupNodeRoundRobin.computeIfAbsent(group, ignored -> new AtomicLong(0));
        int index = Math.floorMod(cursor.getAndIncrement(), sortedNodes.size());
        return sortedNodes.get(index);
    }

    private void replayRetained(Channel channel, String topicFilter) {
        List<RetainedMessage> retainedMessages = retainedMessageStore.findByTopicFilter(topicFilter);
        for (RetainedMessage retained : retainedMessages) {
            channel.writeAndFlush(MqttMessageBuilders.publish()
                    .topicName(retained.getTopic())
                    .retained(true)
                    .qos(MqttQoS.AT_MOST_ONCE)
                    .payload(Unpooled.wrappedBuffer(retained.getPayload()))
                    .build());
        }
    }

    private void rejectConnection(ChannelHandlerContext ctx, MqttConnectReturnCode returnCode) {
        ctx.writeAndFlush(MqttMessageBuilders.connAck().sessionPresent(false).returnCode(returnCode).build())
                .addListener(future -> ctx.close());
    }

    private String currentClientId(Channel channel) {
        String clientId = channel.attr(CLIENT_ID).get();
        if (clientId == null || clientId.isBlank()) {
            return null;
        }
        return clientId;
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value;
    }

    private String safeJson(String value) {
        if (value == null) {
            return "";
        }
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"");
    }

    private boolean allowed(String clientId, String topic, AclAction action) {
        String username = clientId == null ? null : sessionRegistry.get(clientId).map(ClientSession::username).orElse(null);
        return !aclAuthorizer.isAllowed(new AclRequest(clientId, username, topic, action));
    }

    private void applyGlobalRegisterAfterLocal(String topicFilter) {
        if (globalSubscriptionRegistry == null || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        String normalized = SharedSubscription.normalizeTopicFilter(topicFilter);
        String group = shared == null ? null : shared.group();
        long nextIndex = globalRouteLogIndex.incrementAndGet();
        globalSubscriptionRegistry.apply(GlobalSubscriptionEvent.register(nextIndex, nodeId, normalized, group));
    }

    private void applyGlobalUnregisterAfterLocal(String topicFilter) {
        if (globalSubscriptionRegistry == null || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        String normalized = SharedSubscription.normalizeTopicFilter(topicFilter);
        String group = shared == null ? null : shared.group();
        long nextIndex = globalRouteLogIndex.incrementAndGet();
        globalSubscriptionRegistry.apply(GlobalSubscriptionEvent.unregister(nextIndex, nodeId, normalized, group));
    }

    private void submitRetainedAsync(String topic, byte[] payload, int qos) {
        if (!retainedEnabled) {
            return;
        }
        byte[] payloadCopy = payload == null ? new byte[0] : payload.clone();
        retainedStoreExecutor.execute(() -> {
            try {
                retainedMessageStore.saveOrRemove(new RetainedMessage(topic, payloadCopy, qos, true));
            } catch (Exception e) {
                LOG.warning("[RETAINED] async save/remove failed topic=" + topic + ", error=" + e.getMessage());
            }
        });
    }

    public void shutdown() {
        retainedStoreExecutor.shutdown();
        try {
            if (!retainedStoreExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                retainedStoreExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            retainedStoreExecutor.shutdownNow();
        }
    }

    private String resolveServiceNodeIp(Channel channel) {
        String preferred = System.getProperty("jmqx.node.ip");
        if (preferred == null || preferred.isBlank()) {
            preferred = System.getenv("JMQX_NODE_IP");
        }
        if (preferred != null && !preferred.isBlank()) {
            return preferred.trim();
        }

        SocketAddress localAddress = channel.localAddress();
        if (localAddress instanceof InetSocketAddress inetSocketAddress) {
            InetAddress inetAddress = inetSocketAddress.getAddress();
            if (inetAddress != null && !inetAddress.isAnyLocalAddress()) {
                return inetAddress.getHostAddress();
            }
        }

        try {
            InetAddress localHost = InetAddress.getLocalHost();
            if (localHost instanceof Inet4Address) {
                return localHost.getHostAddress();
            }
            return localHost.getHostAddress();
        } catch (Exception ignored) {
            return "unknown";
        }
    }

    /**
     * 连接建立时缓存的遗嘱消息。
     *
     * @author liucaiwen
     * @date 2026/4/7
     */
    private record WillMessage(String topic, byte[] payload, int qos, boolean retain) {
    }
}
