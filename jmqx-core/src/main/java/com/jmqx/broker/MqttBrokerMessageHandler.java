package com.jmqx.broker;

import com.jmqx.admin.AdminReporter;
import com.jmqx.acl.AclAction;
import com.jmqx.acl.AclAuthorizer;
import com.jmqx.acl.AclRequest;
import com.jmqx.bridge.BridgeMessage;
import com.jmqx.bridge.MessageBridge;
import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;
import com.jmqx.common.SharedSubscription;
import com.jmqx.protocol.BrokerMessageHandler;
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.router.SharedSubscriptionManager;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.router.SubscriptionMatchResult;
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
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.AttributeKey;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private static final AttributeKey<Boolean> CLEAN_START = AttributeKey.valueOf("jmqx.cleanStart");
    private static final AttributeKey<String> WS_USERNAME = AttributeKey.valueOf("jmqx.ws.username");
    private static final AttributeKey<String> CONNECTION_TYPE = AttributeKey.valueOf("jmqx.connectionType");
    private static final AttributeKey<Boolean> GRACEFUL_DISCONNECT = AttributeKey.valueOf("jmqx.gracefulDisconnect");
    private static final AttributeKey<WillMessage> WILL_MESSAGE = AttributeKey.valueOf("jmqx.willMessage");

    private static final String TOPIC_CLIENT_CONNECTED = "$SYS/jmqx/events/client/connected";
    private static final String TOPIC_CLIENT_DISCONNECTED = "$SYS/jmqx/events/client/disconnected";
    private static final String DASHBOARD_TOPIC_PREFIX = "$SYS/dashboard/";
    private static final String SUBSCRIPTION_NAMESPACE = "route.subscription";
    private static final String SESSION_NAMESPACE = "session.client";
    private static final String OP_REGISTER = "register";
    private static final String OP_UNREGISTER = "unregister";
    private static final String OP_ONLINE = "online";
    private static final long SESSION_EXPIRY_IMMEDIATE = 0L;
    private static final long SESSION_EXPIRY_PERSISTENT = Long.MAX_VALUE;

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
    private final MetadataCommandGateway metadataCommandGateway;
    private final ClusterMessageDispatcher clusterMessageDispatcher;
    private final AdminReporter adminReporter;
    private final String dashboardClusterId;
    private final List<String> bridgeTopicFilters;
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
            String nodeId,
            MetadataCommandGateway metadataCommandGateway,
            ClusterMessageDispatcher clusterMessageDispatcher,
            AdminReporter adminReporter,
            String dashboardClusterId,
            String bridgeTopicFilters) {
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
        this.metadataCommandGateway = Objects.requireNonNull(metadataCommandGateway, "metadataCommandGateway");
        this.clusterMessageDispatcher = Objects.requireNonNull(clusterMessageDispatcher, "clusterMessageDispatcher");
        this.adminReporter = adminReporter == null ? AdminReporter.NOOP : adminReporter;
        this.dashboardClusterId = (dashboardClusterId == null || dashboardClusterId.isBlank())
            ? "default"
            : dashboardClusterId.trim();
        this.bridgeTopicFilters = parseBridgeTopicFilters(bridgeTopicFilters);
        this.retainedStoreExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "jmqx-retained-store");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void onMessage(ChannelHandlerContext ctx, MqttMessage message) {
        // 协议解码失败直接断开，避免非法报文继续占用连接。
        if (message.decoderResult().isFailure()) {
            LOG.warning(() -> "[PROTO] decode failed, remote=" + ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // 按 MQTT 报文类型分发到对应处理函数。
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
        String clientIp = resolveClientIp(channel);
        boolean cleanStart = Optional.ofNullable(channel.attr(CLEAN_START).get()).orElse(false);
        long sessionExpirySeconds = session == null ? SESSION_EXPIRY_IMMEDIATE : session.sessionExpiryIntervalSeconds();
        boolean gracefulDisconnect = Optional.ofNullable(channel.attr(GRACEFUL_DISCONNECT).get()).orElse(false);

        if (!gracefulDisconnect) {
            publishWillMessageIfPresent(channel, clientId);
        }

        adminReporter.removeClientSession(clientId);
        sessionRegistry.remove(clientId);
        if (shouldClearSessionState(cleanStart, sessionExpirySeconds)) {
            sharedSubscriptionManager.removeClient(clientId);
            Set<String> removedLastTopics = subscriptionRegistry.removeClientAndCollectLastTopics(clientId);
            removedLastTopics.forEach(this::applyGlobalUnregisterAfterLocal);
        }
        publishClientLifecycleEvent(
            TOPIC_CLIENT_DISCONNECTED,
            dashboardClusterId,
            nodeId,
            clientId,
            clientIp,
            username,
            connectionType,
            serviceNodeIp,
            keepAliveSeconds,
            gracefulDisconnect ? "graceful" : "unexpected"
        );
        publishClientLifecycleEvent(
            dashboardTopic("client/disconnected"),
            dashboardClusterId,
            nodeId,
            clientId,
            clientIp,
            username,
            connectionType,
            serviceNodeIp,
            keepAliveSeconds,
            gracefulDisconnect ? "graceful" : "unexpected"
        );
        LOG.fine(() -> "[SESSION] offline clientId=" + clientId
            + ", cleanStart=" + cleanStart + ", sessionExpirySeconds=" + sessionExpirySeconds);
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {
        MqttVersion mqttVersion = resolveMqttVersion(message);
        if (mqttVersion == null) {
            LOG.warning(() -> "[CONNECT] rejected unknown protocol version, remote=" + ctx.channel().remoteAddress()
                + ", version=" + message.variableHeader().version());
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false);
            return;
        }
        boolean mqtt5 = mqttVersion == MqttVersion.MQTT_5;
        if (!mqtt5 && mqttVersion != MqttVersion.MQTT_3_1 && mqttVersion != MqttVersion.MQTT_3_1_1) {
            LOG.warning(() -> "[CONNECT] rejected unsupported protocol version, remote=" + ctx.channel().remoteAddress()
                + ", version=" + message.variableHeader().version());
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false);
            return;
        }

        // 第一步：校验 clientId，空 clientId 直接按协议拒绝，避免无身份连接进入后续流程。
        String clientId = message.payload().clientIdentifier();
        if (clientId == null || clientId.isBlank()) {
            LOG.warning(() -> "[CONNECT] rejected empty clientId, remote=" + ctx.channel().remoteAddress());
            rejectConnection(
                ctx,
                mqtt5
                    ? MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID
                    : MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                mqtt5
            );
            return;
        }

        // 第二步：组装连接上下文（优先使用 MQTT username，其次 WebSocket 握手注入的用户名，最后回退到 clientId）。
        String username = normalize(message.payload().userName());
        if (username == null) {
            username = normalize(ctx.channel().attr(WS_USERNAME).get());
        }
        if (username == null) {
            username = clientId;
        }
        // 连接类型用于管理台展示与后续排障（mqtt / websocket）。
        String connectionType = normalize(ctx.channel().attr(CONNECTION_TYPE).get());
        if (connectionType == null) {
            connectionType = "mqtt";
        }
        // MQTT CONNECT 中密码是二进制字段，这里统一按 UTF-8 解析为字符串供鉴权插件使用。
        String password = message.payload().passwordInBytes() == null
                ? null
                : new String(message.payload().passwordInBytes(), StandardCharsets.UTF_8);

        // 第三步：执行连接鉴权（AUTH 插件链），失败时返回标准 CONNACK 错误码并终止流程。
        if (!clientAuthenticator.authenticate(clientId, username, password)) {
            LOG.warning("[CONNECT] auth failed clientId=" + clientId + ", username=" + username);
            rejectConnection(
                ctx,
                mqtt5
                    ? MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD
                    : MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
                mqtt5
            );
            return;
        }

        // 第四步：构建并注册会话对象，MQTT5 使用 cleanStart + sessionExpiry，MQTT3 兼容 cleanSession 语义。
        boolean cleanStart = message.variableHeader().isCleanSession();
        long sessionExpirySeconds = mqtt5
            ? resolveSessionExpiryIntervalSeconds(message)
            : (cleanStart ? SESSION_EXPIRY_IMMEDIATE : SESSION_EXPIRY_PERSISTENT);
        int keepAliveSeconds = Math.max(message.variableHeader().keepAliveTimeSeconds(), 0);
        String serviceNodeIp = resolveServiceNodeIp(ctx.channel());
        boolean sessionPresent = !cleanStart && sessionRegistry.get(clientId).isPresent();
        sessionRegistry.register(new ClientSession(
            clientId,
            ctx.channel(),
            connectionType,
            cleanStart,
            sessionExpirySeconds,
            username,
            serviceNodeIp,
            keepAliveSeconds,
            Instant.now()
        ));
        // 第四步补充：把“客户端已上线”写入集群元数据，其他节点据此清理同 clientId 旧连接。
        applyGlobalClientOnlineAfterLocalConnect(clientId, System.currentTimeMillis());
        ctx.channel().attr(CLIENT_ID).set(clientId);
        ctx.channel().attr(CLEAN_START).set(cleanStart);
        ctx.channel().attr(GRACEFUL_DISCONNECT).set(false);

        // 第五步：解析并缓存遗嘱消息，供异常断连时发布。
        WillMessage willMessage = buildWillMessage(message);
        if (willMessage != null) {
            ctx.channel().attr(WILL_MESSAGE).set(willMessage);
        } else {
            ctx.channel().attr(WILL_MESSAGE).set(null);
        }

        // 第六步：发送 CONNACK 成功回包，完成 MQTT CONNECT 握手。
        if (mqtt5) {
            ctx.writeAndFlush(MqttMessageBuilders.connAck()
                    .sessionPresent(sessionPresent)
                    .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                    .properties(buildConnAckProperties(sessionExpirySeconds))
                    .build());
        } else {
            ctx.writeAndFlush(MqttMessageBuilders.connAck()
                    .sessionPresent(sessionPresent)
                    .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                    .build());
        }
        // 第七步：发布系统事件到公共主题，供监控与管理台实时订阅。
        publishClientLifecycleEvent(
            TOPIC_CLIENT_CONNECTED,
            dashboardClusterId,
            nodeId,
            clientId,
            resolveClientIp(ctx.channel()),
            username,
            connectionType,
            serviceNodeIp,
            keepAliveSeconds,
            "connected"
        );
        publishClientLifecycleEvent(
            dashboardTopic("client/connected"),
            dashboardClusterId,
            nodeId,
            clientId,
            resolveClientIp(ctx.channel()),
            username,
            connectionType,
            serviceNodeIp,
            keepAliveSeconds,
            "connected"
        );
        // 第八步：刷新管理台会话视图，确保客户端列表可见最新连接信息。
        adminReporter.upsertClientSession(
            clientId,
            nodeId,
            resolveClientIp(ctx.channel()),
            keepAliveSeconds,
            connectionType,
            username,
            Instant.now().toEpochMilli()
        );
        syncClientSubscriptionsForAdmin(clientId);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("[CONNECT] accepted clientId=" + clientId
                + ", connectionType=" + connectionType
                + ", username=" + username
                + ", serviceNodeIp=" + serviceNodeIp
                + ", protocol=" + mqttVersion
                + ", cleanStart=" + cleanStart
                + ", sessionExpirySeconds=" + sessionExpirySeconds
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
            // ACL 鉴权
            if (isDenied(clientId, normalizedFilter, AclAction.SUBSCRIBE)) {
                grantedQos.add(MqttQoS.FAILURE.value());
                LOG.warning(() -> "[ACL] subscribe denied clientId=" + clientId + ", topicFilter=" + topicFilter);
                continue;
            }
            int qos = Math.min(subscription.qualityOfService().value(), 1);
            SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
            if (shared != null && !sharedSubscriptionManager.register(shared.group(), clientId)) {
                grantedQos.add(MqttQoS.FAILURE.value());
                LOG.warning(() -> "[SHARED] subscribe rejected by group limit clientId=" + clientId
                    + ", group=" + shared.group() + ", topicFilter=" + topicFilter);
                continue;
            }
            // 先更新本地订阅，再在首次订阅时同步全局路由。
            boolean firstLocal = subscriptionRegistry.subscribeAndCheckFirst(clientId, topicFilter, qos);
            if (firstLocal) {
                applyGlobalRegisterAfterLocal(topicFilter);
            }
            grantedQos.add(qos);
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
        syncClientSubscriptionsForAdmin(clientId);
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
            boolean lastLocal = subscriptionRegistry.unsubscribeAndCheckLast(clientId, topicFilter);
            if (lastLocal) {
                applyGlobalUnregisterAfterLocal(topicFilter);
            }
        });
        ctx.writeAndFlush(MqttMessageBuilders.unsubAck().packetId(message.variableHeader().messageId()).build());
        syncClientSubscriptionsForAdmin(clientId);
        LOG.fine(() -> "[UNSUBSCRIBE] clientId=" + clientId + ", topics=" + message.payload().topics());
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        byte[] payload = ByteBufUtil.getBytes(message.payload());
        int qos = message.fixedHeader().qosLevel().value();
        String clientId = currentClientId(ctx.channel());
        // ACL 鉴权
        if (isDenied(clientId, topic, AclAction.PUBLISH)) {
            LOG.warning(() -> "[ACL] publish denied clientId=" + clientId + ", topic=" + topic);
            if (qos == 1) {
                ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(message.variableHeader().packetId()).build());
            }
            return;
        }

        // Retain 消息进入异步存储，避免阻塞 I/O 线程。
        if (message.fixedHeader().isRetain()) {
            submitRetainedAsync(topic, payload, qos);
        }

        //  路由消息 发送到指定 client
        routeMessage(topic, payload);

        // 判断是否需要桥接消息
        if (shouldBridgeTopic(topic)) {
            messageBridge.publish(new BridgeMessage(
                clientId,
                topic,
                payload,
                qos,
                message.fixedHeader().isRetain(),
                System.currentTimeMillis()
            ));
        }
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
            String clusterId,
            String nodeId,
            String clientId,
            String clientIp,
            String username,
            String connectionType,
            String serviceNodeIp,
            int keepAliveSeconds,
            String eventType) {
        String payload = "{\"event\":\"" + safeJson(eventType)
            + "\",\"clusterId\":\"" + safeJson(clusterId)
            + "\",\"nodeId\":\"" + safeJson(nodeId)
            + "\",\"clientId\":\"" + safeJson(clientId)
            + "\",\"clientIp\":\"" + safeJson(clientIp)
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

    /**
     * 发布系统主题消息（用于 Dashboard 等内部主题）。
     */
    public void publishSystemTopic(String topic, byte[] payload) {
        if (topic == null || topic.isBlank() || payload == null) {
            return;
        }
        publishInternal(
            "system",
            topic,
            payload,
            MqttQoS.AT_MOST_ONCE.value(),
            false
        );
    }

    private void routeMessage(String topic, byte[] payload) {
        GlobalSubscriptionMatch globalMatch = globalSubscriptionRegistry == null
            ? new GlobalSubscriptionMatch(Collections.emptySet(), Collections.emptyMap())
            : globalSubscriptionRegistry.match(topic);
        // 始终基于本地订阅表进行一次匹配，避免本地订阅已生效但全局路由尚未传播时漏投递。
        SubscriptionMatchResult localMatch = subscriptionRegistry.findSubscriptionMatch(topic);
        Set<String> localSubscribers = new LinkedHashSet<>(localMatch.getDirectSubscribers());
        Map<String, ClusterMessageDispatcher.DispatchTarget> remoteTargetPlans = buildRemoteNormalTargetPlans(globalMatch);
        selectSharedDeliveryTargets(localMatch, globalMatch, localSubscribers, remoteTargetPlans);

        deliverToLocalSubscribers(topic, payload, localSubscribers);
        if (!remoteTargetPlans.isEmpty()) {
            clusterMessageDispatcher.dispatch(topic, payload.clone(), remoteTargetPlans);
            LOG.fine(() -> "[ROUTE][CLUSTER] topic=" + topic + ", remoteTargets=" + remoteTargetPlans.keySet());
        }
    }

    private Map<String, ClusterMessageDispatcher.DispatchTarget> buildRemoteNormalTargetPlans(GlobalSubscriptionMatch globalMatch) {
        if (globalMatch == null || globalMatch.normalNodes().isEmpty()) {
            return new HashMap<>();
        }
        Map<String, ClusterMessageDispatcher.DispatchTarget> plans = new HashMap<>();
        for (String targetNodeId : globalMatch.normalNodes()) {
            if (targetNodeId == null || targetNodeId.isBlank() || targetNodeId.equals(nodeId)) {
                continue;
            }
            plans.put(targetNodeId, ClusterMessageDispatcher.DispatchTarget.normalOnly());
        }
        return plans;
    }



    /**
     * 集群远端消息入口。
     * 远端投递只做本地订阅派发，不再继续向其他节点转发，避免形成环路。
     *
     * @param topic   topic
     * @param payload payload
     */
    public void onClusterPublish(String topic, byte[] payload, boolean includeNormal, Set<String> sharedGroups) {
        if (topic == null || topic.isBlank() || payload == null) {
            return;
        }
        // 路由到本地订阅客户端
        routeLocalOnly(topic, payload, includeNormal, sharedGroups);
    }

    private void routeLocalOnly(String topic, byte[] payload, boolean includeNormal, Set<String> sharedGroups) {
        if (!includeNormal && (sharedGroups == null || sharedGroups.isEmpty())) {
            return;
        }
        SubscriptionMatchResult matchResult = subscriptionRegistry.findSubscriptionMatch(topic);
        Set<String> subscribers = new LinkedHashSet<>();
        if (includeNormal) {
            subscribers.addAll(matchResult.getDirectSubscribers());
        }
        matchResult.getSharedSubscribersByGroup().forEach((group, candidates) -> {
            if (sharedGroups != null && !sharedGroups.contains(group)) {
                return;
            }
            String selected = sharedSubscriptionManager.selectSubscriber(group, candidates, sessionRegistry);
            if (selected != null) {
                subscribers.add(selected);
            }
        });
        deliverToLocalSubscribers(topic, payload, subscribers);
    }

    /**
     * 本地订阅派发
     * @param topic  topic
     * @param payload  payload
     * @param subscribers  subscribers
     */
    private void deliverToLocalSubscribers(String topic, byte[] payload, Set<String> subscribers) {
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
    }

    /**
     * 共享订阅全局投递决策：
     * 每个共享组只选择一个目标（本地客户端或远端节点），避免同组重复投递。
     */
    private void selectSharedDeliveryTargets(
        SubscriptionMatchResult localMatch,
        GlobalSubscriptionMatch globalMatch,
        Set<String> localSubscribers,
        Map<String, ClusterMessageDispatcher.DispatchTarget> remoteTargetPlans
    ) {
        Map<String, Set<String>> localSharedGroups = localMatch.getSharedSubscribersByGroup();
        Map<String, Set<String>> globalSharedGroups = globalMatch.sharedGroupToNodes();
        Set<String> allGroups = new LinkedHashSet<>(globalSharedGroups.keySet());
        allGroups.addAll(localSharedGroups.keySet());
        for (String group : allGroups) {
            Set<String> localCandidates = localSharedGroups.getOrDefault(group, Collections.emptySet());
            Set<String> globalNodes = globalSharedGroups.getOrDefault(group, Collections.emptySet());
            SharedDeliveryTarget target = selectSharedDeliveryTarget(group, localCandidates, globalNodes);
            if (target == null) {
                continue;
            }
            if (target.localClientId() != null) {
                localSubscribers.add(target.localClientId());
            } else if (target.remoteNodeId() != null) {
                mergeSharedTargetPlan(remoteTargetPlans, target.remoteNodeId(), group);
            }
        }
    }

    private void mergeSharedTargetPlan(
        Map<String, ClusterMessageDispatcher.DispatchTarget> remoteTargetPlans,
        String targetNodeId,
        String sharedGroup
    ) {
        if (targetNodeId == null || targetNodeId.isBlank() || targetNodeId.equals(nodeId)) {
            return;
        }
        ClusterMessageDispatcher.DispatchTarget oldPlan = remoteTargetPlans.get(targetNodeId);
        Set<String> mergedGroups = new LinkedHashSet<>();
        boolean includeNormal = false;
        if (oldPlan != null) {
            includeNormal = oldPlan.includeNormal();
            mergedGroups.addAll(oldPlan.sharedGroups());
        }
        if (sharedGroup != null && !sharedGroup.isBlank()) {
            mergedGroups.add(sharedGroup);
        }
        remoteTargetPlans.put(targetNodeId, new ClusterMessageDispatcher.DispatchTarget(includeNormal, mergedGroups));
    }

    /**
     * 共享订阅目标选择
     * @param group 共享订阅组
     * @param localCandidates 本地客户端候选
     * @param globalNodes 全局节点候选
     * @return 目标
     */
    private SharedDeliveryTarget selectSharedDeliveryTarget(
        String group,
        Set<String> localCandidates,
        Set<String> globalNodes
    ) {
        if (group == null || group.isBlank()) {
            return null;
        }
        Set<String> nodeCandidates = new LinkedHashSet<>(globalNodes);
        if (localCandidates != null && !localCandidates.isEmpty()) {
            nodeCandidates.add(nodeId);
        }
        if (nodeCandidates.isEmpty()) {
            return null;
        }
        List<String> sortedNodes = new ArrayList<>(nodeCandidates);
        Collections.sort(sortedNodes);
        AtomicLong cursor = sharedGroupNodeRoundRobin.computeIfAbsent(group, ignored -> new AtomicLong(0));
        int start = Math.floorMod(cursor.getAndIncrement(), sortedNodes.size());
        for (int i = 0; i < sortedNodes.size(); i++) {
            String selectedNode = sortedNodes.get((start + i) % sortedNodes.size());
            if (selectedNode.equals(nodeId)) {
                String localClient = sharedSubscriptionManager.selectSubscriber(group, localCandidates, sessionRegistry);
                if (localClient != null) {
                    return SharedDeliveryTarget.local(localClient);
                }
                continue;
            }
            return SharedDeliveryTarget.remote(selectedNode);
        }
        return null;
    }

    private void replayRetained(Channel channel, String topicFilter) {
        List<RetainedMessage> retainedMessages = retainedMessageStore.findByTopicFilter(topicFilter);
        for (RetainedMessage retained : retainedMessages) {
            channel.writeAndFlush(MqttMessageBuilders.publish()
                    .topicName(retained.topic())
                    .retained(true)
                    .qos(MqttQoS.AT_MOST_ONCE)
                    .payload(Unpooled.wrappedBuffer(retained.payload()))
                    .build());
        }
    }

    private void rejectConnection(ChannelHandlerContext ctx, MqttConnectReturnCode returnCode, boolean mqtt5) {
        MqttMessageBuilders.ConnAckBuilder builder = MqttMessageBuilders.connAck()
            .sessionPresent(false)
            .returnCode(returnCode);
        if (mqtt5) {
            builder.properties(MqttProperties.NO_PROPERTIES);
        }
        ctx.writeAndFlush(builder.build()).addListener(future -> ctx.close());
    }

    private MqttVersion resolveMqttVersion(MqttConnectMessage message) {
        if (message == null || message.variableHeader() == null) {
            return null;
        }
        try {
            return MqttVersion.fromProtocolNameAndLevel(
                message.variableHeader().name(),
                (byte) message.variableHeader().version()
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private long resolveSessionExpiryIntervalSeconds(MqttConnectMessage message) {
        if (message == null || message.variableHeader() == null) {
            return SESSION_EXPIRY_IMMEDIATE;
        }
        MqttProperties properties = message.variableHeader().properties();
        if (properties == null || properties.isEmpty()) {
            return SESSION_EXPIRY_IMMEDIATE;
        }
        MqttProperties.MqttProperty<?> property = properties.getProperty(
            MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value()
        );
        if (property == null) {
            return SESSION_EXPIRY_IMMEDIATE;
        }
        Object value = property.value();
        if (!(value instanceof Integer seconds)) {
            return SESSION_EXPIRY_IMMEDIATE;
        }
        return Math.max(0L, seconds.longValue());
    }

    private boolean shouldClearSessionState(boolean cleanStart, long sessionExpirySeconds) {
        return cleanStart || sessionExpirySeconds == SESSION_EXPIRY_IMMEDIATE;
    }

    private MqttProperties buildConnAckProperties(long sessionExpirySeconds) {
        MqttProperties properties = new MqttProperties();
        properties.add(new MqttProperties.IntegerProperty(
            MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(),
            (int) Math.max(0L, Math.min(Integer.MAX_VALUE, sessionExpirySeconds))
        ));
        return properties;
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

    private void syncClientSubscriptionsForAdmin(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        try {
            List<String> topics = new ArrayList<>(subscriptionRegistry.findSubscriptions(clientId).keySet());
            adminReporter.upsertClientSubscriptions(clientId, topics);
        } catch (Exception exception) {
            LOG.warning("[ADMIN] sync subscriptions failed, clientId=" + clientId + ", error=" + exception.getMessage());
        }
    }

    private String resolveClientIp(Channel channel) {
        if (channel == null) {
            return "unknown";
        }
        SocketAddress remote = channel.remoteAddress();
        if (remote instanceof InetSocketAddress socketAddress) {
            InetAddress address = socketAddress.getAddress();
            if (address != null) {
                return address.getHostAddress();
            }
            return socketAddress.getHostString();
        }
        return "unknown";
    }

    private String safeJson(String value) {
        if (value == null) {
            return "";
        }
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"");
    }

    private String dashboardTopic(String suffix) {
        return DASHBOARD_TOPIC_PREFIX + dashboardClusterId + "/" + suffix;
    }

    /**
     * 桥接主题判定：
     * 1. 配置了 topicFilters：仅桥接匹配过滤器的主题；
     * 2. 未配置 topicFilters：桥接所有非 dashboard 主题。
     */
    private boolean shouldBridgeTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            return false;
        }
        if (bridgeTopicFilters.isEmpty()) {
            return !topic.startsWith(DASHBOARD_TOPIC_PREFIX);
        }
        for (String filter : bridgeTopicFilters) {
            if (matchesTopicFilter(filter, topic)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> parseBridgeTopicFilters(String raw) {
        if (raw == null || raw.isBlank()) {
            return Collections.emptyList();
        }
        List<String> filters = new ArrayList<>();
        for (String token : raw.split(",")) {
            if (token == null) {
                continue;
            }
            String filter = token.trim();
            if (!filter.isBlank()) {
                filters.add(filter);
            }
        }
        return filters.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(filters);
    }

    private static boolean matchesTopicFilter(String filter, String topic) {
        if (filter == null || filter.isBlank() || topic == null || topic.isBlank()) {
            return false;
        }
        String[] filterLevels = filter.split("/", -1);
        String[] topicLevels = topic.split("/", -1);
        int fi = 0;
        int ti = 0;
        while (fi < filterLevels.length && ti < topicLevels.length) {
            String level = filterLevels[fi];
            if ("#".equals(level)) {
                return fi == filterLevels.length - 1;
            }
            if ("+".equals(level) || level.equals(topicLevels[ti])) {
                fi++;
                ti++;
                continue;
            }
            return false;
        }
        if (fi == filterLevels.length && ti == topicLevels.length) {
            return true;
        }
        return fi == filterLevels.length - 1 && "#".equals(filterLevels[fi]);
    }

    private boolean isDenied(String clientId, String topic, AclAction action) {
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
        long committedIndex = metadataCommandGateway.submit(new MetadataCommand(
            SUBSCRIPTION_NAMESPACE,
            OP_REGISTER,
            normalized,
            group,
            nodeId
        ));
        if (committedIndex >= 0) {
            return;
        }
        LOG.warning(() -> "[CLUSTER] metadata submit failed, skip global-register apply, topicFilter=" + topicFilter);
    }

    private void applyGlobalUnregisterAfterLocal(String topicFilter) {
        if (globalSubscriptionRegistry == null || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        String normalized = SharedSubscription.normalizeTopicFilter(topicFilter);
        String group = shared == null ? null : shared.group();
        long committedIndex = metadataCommandGateway.submit(new MetadataCommand(
            SUBSCRIPTION_NAMESPACE,
            OP_UNREGISTER,
            normalized,
            group,
            nodeId
        ));
        if (committedIndex >= 0) {
            return;
        }
        LOG.warning(() -> "[CLUSTER] metadata submit failed, skip global-unregister apply, topicFilter=" + topicFilter);
    }

    /**
     * CONNECT 成功后上报 client-online 事件，驱动集群内 clientId 唯一会话。
     * @param clientId clientId
     * @param connectedAtMs 连接时间
     */
    private void applyGlobalClientOnlineAfterLocalConnect(String clientId, long connectedAtMs) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        long committedIndex = metadataCommandGateway.submit(new MetadataCommand(
            SESSION_NAMESPACE,
            OP_ONLINE,
            clientId,
            String.valueOf(Math.max(0L, connectedAtMs)),
            nodeId
        ));
        if (committedIndex >= 0) {
            return;
        }
        LOG.warning(() -> "[CLUSTER] metadata submit failed, skip session-online apply, clientId=" + clientId);
    }

    /**
     * 提交保留消息
     * @param topic topic
     * @param payload payload
     * @param qos qos
     */
    private void submitRetainedAsync(String topic, byte[] payload, int qos) {
        // 关闭 retain 能力时直接返回。
        if (!retainedEnabled) {
            return;
        }
        byte[] payloadCopy = payload == null ? new byte[0] : payload.clone();
        // 通过单线程后台队列串行写入 retained 存储。
        retainedStoreExecutor.execute(() -> {
            try {
                retainedMessageStore.saveOrRemove(new RetainedMessage(topic, payloadCopy, qos, true));
            } catch (Exception e) {
                LOG.warning("[RETAINED] async save/remove failed topic=" + topic + ", error=" + e.getMessage());
            }
        });
    }

    public void shutdown() {
        adminReporter.shutdown();
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

    private record SharedDeliveryTarget(String localClientId, String remoteNodeId) {
        private static SharedDeliveryTarget local(String localClientId) {
            return new SharedDeliveryTarget(localClientId, null);
        }

        private static SharedDeliveryTarget remote(String remoteNodeId) {
            return new SharedDeliveryTarget(null, remoteNodeId);
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
