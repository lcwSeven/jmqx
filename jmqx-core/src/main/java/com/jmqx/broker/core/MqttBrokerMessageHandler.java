package com.jmqx.broker.core;

import com.jmqx.admin.AdminReporter;
import com.jmqx.acl.AclAction;
import com.jmqx.acl.AclAuthorizer;
import com.jmqx.acl.AclRequest;
import com.jmqx.broker.protocol.MqttPacketFactory;
import com.jmqx.broker.qos.BrokerInflightManager;
import com.jmqx.broker.qos.InboundQos2Publish;
import com.jmqx.broker.ratelimit.BrokerRateLimitConfig;
import com.jmqx.broker.ratelimit.BrokerRateLimiter;
import com.jmqx.broker.retained.RetainedCommandReplicator;
import com.jmqx.bridge.BridgeMessage;
import com.jmqx.bridge.MessageBridge;
import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;
import com.jmqx.common.SharedSubscription;
import com.jmqx.common.logging.ClientLogContext;
import com.jmqx.protocol.BrokerMessageHandler;
import com.jmqx.protocol.AuthDecision;
import com.jmqx.protocol.AuthResult;
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.protocol.ClientBlacklist;
import com.jmqx.router.SharedSubscriptionManager;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.router.SubscriptionMatchResult;
import com.jmqx.router.global.GlobalSubscriptionMatch;
import com.jmqx.router.global.GlobalSubscriptionRegistry;
import com.jmqx.session.ClientSession;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.retained.RetainedMessage;
import com.jmqx.store.retained.RetainedMessageStore;
import com.jmqx.store.qos.Qos1InflightStore;
import com.jmqx.store.qos.Qos2InflightStore;
import com.jmqx.store.will.WillMessageStore;
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
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
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
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
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
    private static final String OP_AUTH_CACHE_EVICT = "auth_cache_evict";
    private static final long SESSION_EXPIRY_IMMEDIATE = 0L;
    private static final long SESSION_EXPIRY_PERSISTENT = Long.MAX_VALUE;
    private static final long RETAINED_RETRY_INTERVAL_MS = 2000L;
    private static final long METADATA_COMMAND_RETRY_INTERVAL_MS = 2000L;
    private static final long ADMIN_SUBSCRIPTIONS_SYNC_DEBOUNCE_MS = 300L;
    private static final long GLOBAL_MATCH_CACHE_TTL_MS = 200L;
    private static final int GLOBAL_MATCH_CACHE_MAX_TOPICS = 16_384;
    private static final GlobalSubscriptionMatch EMPTY_GLOBAL_MATCH =
        new GlobalSubscriptionMatch(Collections.emptySet(), Collections.emptyMap());

    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private final RetainedMessageStore retainedMessageStore;
    private final WillMessageStore willMessageStore;
    private final boolean willPersistenceEnabled;
    private final BrokerInflightManager inflightManager;
    private final ClientAuthenticator clientAuthenticator;
    private final ClientBlacklist clientBlacklist;
    private final AclAuthorizer aclAuthorizer;
    private final SharedSubscriptionManager sharedSubscriptionManager;
    private final MessageBridge messageBridge;
    private volatile boolean bridgeEnabled;
    private final boolean retainedEnabled;
    private final GlobalSubscriptionRegistry globalSubscriptionRegistry;
    private final String nodeId;
    private final MetadataCommandGateway metadataCommandGateway;
    private final ClusterMessageDispatcher clusterMessageDispatcher;
    private final AdminReporter adminReporter;
    private final String dashboardClusterId;
    private volatile List<String> bridgeTopicFilters;
    private final int maxAllowedQos;
    private final int maxSubscriptionsPerClient;
    private final int maxWillPayloadBytes;
    private final ConcurrentMap<String, AtomicLong> sharedGroupNodeRoundRobin = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SharedGroupNodeOrderSnapshot> sharedGroupNodeOrderSnapshots = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CachedGlobalMatch> globalMatchCache = new ConcurrentHashMap<>();
    private final ThreadLocal<RouteScratch> routeScratchHolder = ThreadLocal.withInitial(RouteScratch::new);
    private final ThreadLocal<DeliveryScratch> deliveryScratchHolder = ThreadLocal.withInitial(DeliveryScratch::new);
    private final RetainedCommandReplicator retainedCommandReplicator;
    private final BrokerRateLimiter rateLimiter;
    private final ScheduledExecutorService maintenanceExecutor;
    private final ConcurrentMap<String, PendingMetadataCommand> pendingMetadataCommands = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> adminSubscriptionsSyncDeadlineByClient = new ConcurrentHashMap<>();


    public MqttBrokerMessageHandler(
            SessionRegistry sessionRegistry,
            SubscriptionRegistry subscriptionRegistry,
            RetainedMessageStore retainedMessageStore,
            ClientAuthenticator clientAuthenticator,
            ClientBlacklist clientBlacklist,
            AclAuthorizer aclAuthorizer,
            SharedSubscriptionManager sharedSubscriptionManager,
            MessageBridge messageBridge,
            boolean bridgeEnabled,
            boolean retainedEnabled,
            Qos1InflightStore qos1InflightStore,
            Qos2InflightStore qos2InflightStore,
            WillMessageStore willMessageStore,
            int maxWillPayloadBytes,
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            String nodeId,
            MetadataCommandGateway metadataCommandGateway,
            ClusterMessageDispatcher clusterMessageDispatcher,
            AdminReporter adminReporter,
            String dashboardClusterId,
            String bridgeTopicFilters,
            int maxAllowedQos,
            int maxSubscriptionsPerClient,
            BrokerRateLimitConfig rateLimitConfig) {
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.retainedMessageStore = retainedMessageStore;
        this.clientAuthenticator = clientAuthenticator;
        this.clientBlacklist = clientBlacklist == null ? ClientBlacklist.NOOP : clientBlacklist;
        this.aclAuthorizer = aclAuthorizer;
        this.sharedSubscriptionManager = sharedSubscriptionManager == null
            ? new SharedSubscriptionManager()
            : sharedSubscriptionManager;
        this.messageBridge = messageBridge == null ? MessageBridge.NOOP : messageBridge;
        this.bridgeEnabled = bridgeEnabled;
        this.retainedEnabled = retainedEnabled;
        this.willMessageStore = willMessageStore == null ? WillMessageStore.NOOP : willMessageStore;
        this.willPersistenceEnabled = this.willMessageStore != WillMessageStore.NOOP;
        this.inflightManager = new BrokerInflightManager(qos1InflightStore, qos2InflightStore, LOG);
        this.globalSubscriptionRegistry = globalSubscriptionRegistry;
        this.nodeId = (nodeId == null || nodeId.isBlank()) ? "node-1" : nodeId;
        this.metadataCommandGateway = Objects.requireNonNull(metadataCommandGateway, "metadataCommandGateway");
        this.clusterMessageDispatcher = Objects.requireNonNull(clusterMessageDispatcher, "clusterMessageDispatcher");
        this.adminReporter = adminReporter == null ? AdminReporter.NOOP : adminReporter;
        this.dashboardClusterId = (dashboardClusterId == null || dashboardClusterId.isBlank())
            ? "default"
            : dashboardClusterId.trim();
        this.bridgeTopicFilters = parseBridgeTopicFilters(bridgeTopicFilters);
        this.maxAllowedQos = normalizeMaxQos(maxAllowedQos);
        this.maxSubscriptionsPerClient = Math.max(1, maxSubscriptionsPerClient);
        this.maxWillPayloadBytes = maxWillPayloadBytes <= 0 ? 1024 * 1024 : maxWillPayloadBytes;
        this.rateLimiter = new BrokerRateLimiter(rateLimitConfig);
        this.retainedCommandReplicator = new RetainedCommandReplicator(this.metadataCommandGateway, this.nodeId, LOG);
        this.maintenanceExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "jmqx-maintenance");
            thread.setDaemon(true);
            return thread;
        });
        this.maintenanceExecutor.scheduleAtFixedRate(
            this::runPeriodicMaintenance,
            RETAINED_RETRY_INTERVAL_MS,
            RETAINED_RETRY_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
    }

    public void updateBridgeSettings(boolean enabled, String rawTopicFilters) {
        this.bridgeEnabled = enabled;
        this.bridgeTopicFilters = parseBridgeTopicFilters(rawTopicFilters);
    }

    @Override
    public void onMessage(ChannelHandlerContext ctx, MqttMessage message) {
        // 协议解码失败直接断开，避免非法报文继续占用连接。
        if (message.decoderResult().isFailure()) {
            LOG.warning(() -> "[PROTO] decode failed, remote=" + ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        try (ClientLogContext.Scope ignored = ClientLogContext.open(resolveLoggingClientId(ctx.channel(), message))) {
            // 按 MQTT 报文类型分发到对应处理函数。
            MqttMessageType messageType = message.fixedHeader().messageType();
            switch (messageType) {
                case CONNECT -> handleConnect(ctx, (MqttConnectMessage) message);
                case SUBSCRIBE -> handleSubscribe(ctx, (MqttSubscribeMessage) message);
                case UNSUBSCRIBE -> handleUnsubscribe(ctx, (MqttUnsubscribeMessage) message);
                case PUBLISH -> handlePublish(ctx, (MqttPublishMessage) message);
                case PUBACK -> handlePubAck(ctx, (MqttPubAckMessage) message);
                case PUBREC -> handlePubRec(ctx, message);
                case PUBREL -> handlePubRel(ctx, message);
                case PUBCOMP -> handlePubComp(ctx, message);
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
    }

    @Override
    public void onDisconnect(Channel channel) {
        String clientId = channel.attr(CLIENT_ID).get();
        if (clientId == null || clientId.isBlank()) {
            return;
        }

        try (ClientLogContext.Scope ignored = ClientLogContext.open(clientId)) {
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
            willMessageStore.remove(clientId);

            clientAuthenticator.evictCache(clientId, username);
            applyGlobalClientAuthCacheEvictAfterDisconnect(clientId, username);
            adminReporter.removeClientSession(clientId);
            sessionRegistry.remove(clientId);
            inflightManager.removeRuntimeState(clientId);
            if (shouldClearSessionState(cleanStart, sessionExpirySeconds)) {
                inflightManager.clearPersistentState(clientId);
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
        if (isConnectRateLimited(ctx, mqtt5)) {
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
        String clientIp = resolveClientIp(ctx.channel());

        if (clientBlacklist.isBlocked(clientId, clientIp)) {
            LOG.warning(() -> "[CONNECT] blacklisted client rejected, clientId=" + clientId + ", clientIp=" + clientIp);
            rejectConnection(
                ctx,
                mqtt5
                    ? MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED
                    : MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED,
                mqtt5
            );
            return;
        }

        // 第三步：执行连接鉴权（AUTH 插件链），失败时返回标准 CONNACK 错误码并终止流程。
        AuthResult authResult = clientAuthenticator.authenticateResult(clientId, username, password);
        if (authResult.decision() != AuthDecision.ALLOW) {
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

        // 第四步：解析会话参数，MQTT5 使用 cleanStart + sessionExpiry，MQTT3 兼容 cleanSession 语义。
        boolean cleanStart = message.variableHeader().isCleanSession();
        long sessionExpirySeconds = mqtt5
            ? resolveSessionExpiryIntervalSeconds(message)
            : (cleanStart ? SESSION_EXPIRY_IMMEDIATE : SESSION_EXPIRY_PERSISTENT);
        int keepAliveSeconds = Math.max(message.variableHeader().keepAliveTimeSeconds(), 0);
        String serviceNodeIp = resolveServiceNodeIp(ctx.channel());
        boolean sessionPresent = !cleanStart && inflightManager.hasPersistedSessionState(clientId, subscriptionRegistry);
        ctx.channel().attr(CLIENT_ID).set(clientId);
        ctx.channel().attr(CLEAN_START).set(cleanStart);
        ctx.channel().attr(GRACEFUL_DISCONNECT).set(false);

        // 第五步：解析并校验遗嘱消息大小，超限则拒绝连接，避免大遗嘱导致内存风险。
        if (isWillPayloadTooLarge(message)) {
            LOG.warning(() -> "[CONNECT] rejected oversize will payload, clientId=" + clientId
                + ", maxWillPayloadBytes=" + maxWillPayloadBytes);
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED, mqtt5);
            return;
        }
        // 第六步：解析并缓存/持久化遗嘱消息，供异常断连时发布。
        WillMessage willMessage = buildWillMessage(message);
        if (willMessage != null) {
            if (willPersistenceEnabled) {
                // 持久化模式下不常驻内存，断连时按需从存储读取，降低连接规模放大时的堆占用。
                ctx.channel().attr(WILL_MESSAGE).set(null);
                willMessageStore.save(clientId, willMessage);
            } else {
                ctx.channel().attr(WILL_MESSAGE).set(willMessage);
            }
        } else {
            ctx.channel().attr(WILL_MESSAGE).set(null);
            willMessageStore.remove(clientId);
        }

        // 第五步：发送 CONNACK 成功回包，完成 MQTT CONNECT 握手。
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
        // 第六步：在会话对外可见前恢复 inflight 状态，避免恢复窗口内 packetId 竞争。
        inflightManager.restoreInflightState(clientId, sessionPresent, ctx.channel());

        // 第七步：恢复完成后再注册会话并广播上线，形成稳定连接状态。
        Instant connectedAt = Instant.now();
        sessionRegistry.register(new ClientSession(
            clientId,
            ctx.channel(),
            connectionType,
            cleanStart,
            sessionExpirySeconds,
            username,
            serviceNodeIp,
            keepAliveSeconds,
            authResult.superuser(),
            connectedAt
        ));
        applyGlobalClientOnlineAfterLocalConnect(clientId, connectedAt.toEpochMilli());

        // 第八步：发布系统事件到公共主题，供监控与管理台实时订阅。
        publishClientLifecycleEvent(
            TOPIC_CLIENT_CONNECTED,
            dashboardClusterId,
            nodeId,
            clientId,
            clientIp,
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
            clientIp,
            username,
            connectionType,
            serviceNodeIp,
            keepAliveSeconds,
            "connected"
        );
        // 第九步：刷新管理台会话视图，确保客户端列表可见最新连接信息。
        adminReporter.upsertClientSession(
            clientId,
            nodeId,
            clientIp,
            keepAliveSeconds,
            connectionType,
            username,
            connectedAt.toEpochMilli()
        );
        scheduleClientSubscriptionsSync(clientId);
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

        Map<String, Integer> existingSubscriptions = subscriptionRegistry.findSubscriptions(clientId);
        int projectedSubscriptionCount = existingSubscriptions.size();
        Set<String> packetNewSubscriptions = new LinkedHashSet<>();
        List<Integer> grantedQos = new ArrayList<>();
        List<String> firstLocalTopicFilters = new ArrayList<>();
        Set<String> retainedReplayFilters = retainedEnabled ? new LinkedHashSet<>() : Collections.emptySet();
        for (MqttTopicSubscription subscription : message.payload().topicSubscriptions()) {
            String topicFilter = subscription.topicFilter();
            String normalizedFilter = SharedSubscription.normalizeTopicFilter(topicFilter);
            boolean isExisting = existingSubscriptions.containsKey(topicFilter) || packetNewSubscriptions.contains(topicFilter);
            if (!isExisting && projectedSubscriptionCount >= maxSubscriptionsPerClient) {
                grantedQos.add(MqttQoS.FAILURE.value());
                LOG.warning(() -> "[SUBSCRIBE] rejected by maxSubscriptionsPerClient, clientId=" + clientId
                        + ", topicFilter=" + topicFilter + ", limit=" + maxSubscriptionsPerClient);
                continue;
            }
            // ACL 鉴权
            if (isDenied(clientId, normalizedFilter, AclAction.SUBSCRIBE)) {
                grantedQos.add(MqttQoS.FAILURE.value());
                LOG.warning(() -> "[ACL] subscribe denied clientId=" + clientId + ", topicFilter=" + topicFilter);
                continue;
            }
            int qos = Math.min(subscription.qualityOfService().value(), 2);
            final int effectiveQos = capByMaxQos(normalizeQos(qos));
            SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
            if (shared != null && !sharedSubscriptionManager.register(shared.group(), clientId)) {
                grantedQos.add(MqttQoS.FAILURE.value());
                LOG.warning(() -> "[SHARED] subscribe rejected by group limit clientId=" + clientId
                    + ", group=" + shared.group() + ", topicFilter=" + topicFilter);
                continue;
            }
            // 先更新本地订阅，再在首次订阅时同步全局路由。
            boolean firstLocal = subscriptionRegistry.subscribeAndCheckFirst(clientId, topicFilter, effectiveQos);
            if (firstLocal) {
                firstLocalTopicFilters.add(topicFilter);
            }
            if (!isExisting) {
                packetNewSubscriptions.add(topicFilter);
                projectedSubscriptionCount++;
            }
            grantedQos.add(effectiveQos);
            if (retainedEnabled) {
                retainedReplayFilters.add(normalizedFilter);
            }
            LOG.info(() -> "[SUBSCRIBE] clientId=" + clientId + ", topicFilter=" + topicFilter
                    + ", qos=" + qos + ", effectiveQos=" + effectiveQos);
        }

        MqttMessageBuilders.SubAckBuilder subAckBuilder = MqttMessageBuilders.subAck()
                .packetId(message.variableHeader().messageId());

        grantedQos.forEach(qos -> {
            MqttQoS mqttQoS = MqttQoS.valueOf(qos);
            subAckBuilder.addGrantedQos(mqttQoS);
        });
        ctx.write(subAckBuilder.build());
        if (retainedEnabled && !retainedReplayFilters.isEmpty()) {
            replayRetainedBatch(ctx.channel(), retainedReplayFilters);
        }
        ctx.flush();
        submitGlobalRegisterBatchAsync(firstLocalTopicFilters);
        scheduleClientSubscriptionsSync(clientId);
    }

    private void handleUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null) {
            ctx.close();
            return;
        }

        List<String> topics = message.payload().topics();
        topics.forEach(topicFilter -> {
            SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
            if (shared != null) {
                sharedSubscriptionManager.unregister(shared.group(), clientId);
            }
        });
        Set<String> lastLocalTopics = subscriptionRegistry.unsubscribeBatchAndCollectLast(clientId, topics);
        submitGlobalUnregisterBatchAsync(lastLocalTopics);
        ctx.writeAndFlush(MqttMessageBuilders.unsubAck().packetId(message.variableHeader().messageId()).build());
        scheduleClientSubscriptionsSync(clientId);
        LOG.fine(() -> "[UNSUBSCRIBE] clientId=" + clientId + ", topics=" + topics);
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        byte[] payload = ByteBufUtil.getBytes(message.payload());
        int qos = normalizeQos(message.fixedHeader().qosLevel().value());
        int effectiveQos = capByMaxQos(qos);
        int packetId = message.variableHeader().packetId();
        String clientId = currentClientId(ctx.channel());
        if (qos > 0 && packetId <= 0) {
            LOG.warning(() -> "[PUBLISH] invalid packetId clientId=" + clientId + ", qos=" + qos);
            ctx.close();
            return;
        }
        if (isPublishRateLimited(ctx, clientId, qos, packetId, topic)) {
            return;
        }
        // ACL 鉴权
        if (isDenied(clientId, topic, AclAction.PUBLISH)) {
            handleDeniedPublish(ctx, clientId, topic, qos, packetId);
            return;
        }

        if (qos == 2) {
            handleInboundQos2Publish(ctx, clientId, topic, payload, packetId, message.fixedHeader().isRetain());
            return;
        }

        processApplicationPublish(clientId, topic, payload, effectiveQos, message.fixedHeader().isRetain());
        LOG.fine(() -> "[PUBLISH] clientId=" + clientId + ", topic=" + topic
                + ", qos=" + qos + ", effectiveQos=" + effectiveQos
                + ", retain=" + message.fixedHeader().isRetain() + ", bytes=" + payload.length);

        if (qos == 1) {
            ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(packetId).build());
        }
    }

    /**
     * ACL 拒绝时按 QoS 返回协议应答。
     */
    private void handleDeniedPublish(ChannelHandlerContext ctx, String clientId, String topic, int qos, int packetId) {
        LOG.warning(() -> "[ACL] publish denied clientId=" + clientId + ", topic=" + topic);
        if (qos == 1) {
            ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(packetId).build());
            return;
        }
        if (qos == 2) {
            ctx.writeAndFlush(MqttPacketFactory.buildPubRecMessage(packetId));
        }
    }

    /**
     * QoS2 第一阶段：缓存 PUBLISH 并回复 PUBREC，等待 PUBREL 后再真正投递。
     */
    private void handleInboundQos2Publish(
        ChannelHandlerContext ctx,
        String clientId,
        String topic,
        byte[] payload,
        int packetId,
        boolean retain
    ) {
        inflightManager.saveInboundQos2(clientId, packetId, topic, payload, retain);
        ctx.writeAndFlush(MqttPacketFactory.buildPubRecMessage(packetId));
    }

    /**
     * 普通业务发布处理（QoS0/QoS1 直接投递，QoS2 在 PUBREL 阶段调用）。
     */
    private void processApplicationPublish(String clientId, String topic, byte[] payload, int qos, boolean retain) {
        if (retain && retainedEnabled) {
            retainedCommandReplicator.submitRetainedWithClusterSync(topic, payload, qos);
        }
        routeMessage(topic, payload, qos);
        if (shouldBridgeTopic(topic)) {
            messageBridge.publish(new BridgeMessage(
                clientId,
                topic,
                payload,
                qos,
                retain,
                System.currentTimeMillis()
            ));
        }
    }

    private void handlePubAck(ChannelHandlerContext ctx, MqttPubAckMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null || message == null || message.variableHeader() == null) {
            return;
        }
        int packetId = message.variableHeader().messageId();
        inflightManager.onPubAck(clientId, packetId);
    }

    private void handlePubRec(ChannelHandlerContext ctx, MqttMessage message) {
        String clientId = currentClientId(ctx.channel());
        Integer packetId = MqttPacketFactory.extractPacketId(message);
        if (clientId == null || packetId == null) {
            return;
        }
        if (inflightManager.onPubRec(clientId, packetId)) {
            ctx.writeAndFlush(MqttPacketFactory.buildPubRelMessage(packetId));
        }
    }

    private void handlePubRel(ChannelHandlerContext ctx, MqttMessage message) {
        String clientId = currentClientId(ctx.channel());
        Integer packetId = MqttPacketFactory.extractPacketId(message);
        if (clientId == null || packetId == null) {
            return;
        }
        InboundQos2Publish publish = inflightManager.onPubRel(clientId, packetId);
        if (publish != null) {
            processApplicationPublish(
                clientId,
                publish.topic(),
                publish.payload(),
                capByMaxQos(MqttQoS.EXACTLY_ONCE.value()),
                publish.retain()
            );
        }
        ctx.writeAndFlush(MqttPacketFactory.buildPubCompMessage(packetId));
    }

    private void handlePubComp(ChannelHandlerContext ctx, MqttMessage message) {
        String clientId = currentClientId(ctx.channel());
        Integer packetId = MqttPacketFactory.extractPacketId(message);
        if (clientId == null || packetId == null) {
            return;
        }
        inflightManager.onPubComp(clientId, packetId);
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
        int qos = capByMaxQos(normalizeQos(connectMessage.variableHeader().willQos()));
        boolean retain = connectMessage.variableHeader().isWillRetain();
        return new WillMessage(topic, payload.clone(), qos, retain);
    }

    private void publishWillMessageIfPresent(Channel channel, String clientId) {
        WillMessage willMessage = channel.attr(WILL_MESSAGE).get();
        if (willMessage == null && clientId != null && !clientId.isBlank()) {
            willMessage = willMessageStore.get(clientId).orElse(null);
        }
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
        final WillMessage publishedWillMessage = willMessage;
        LOG.fine(() -> "[WILL] published clientId=" + clientId + ", topic=" + publishedWillMessage.topic());
    }

    private boolean isWillPayloadTooLarge(MqttConnectMessage connectMessage) {
        if (connectMessage == null || connectMessage.payload() == null || !connectMessage.variableHeader().isWillFlag()) {
            return false;
        }
        byte[] payload = connectMessage.payload().willMessageInBytes();
        int payloadLength = payload == null ? 0 : payload.length;
        return payloadLength > maxWillPayloadBytes;
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
        processApplicationPublish(sourceClientId, topic, payload, qos, retain);
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

    private void routeMessage(String topic, byte[] payload, int publishQos) {
        // 匹配全局路由
        GlobalSubscriptionMatch globalMatch = resolveGlobalMatch(topic);
        RouteScratch routeScratch = routeScratchHolder.get();
        Set<String> localSubscribers = routeScratch.localSubscribers();
        Map<String, ClusterMessageDispatcher.DispatchTarget> remoteTargetPlans = routeScratch.remoteTargetPlans();
        localSubscribers.clear();
        remoteTargetPlans.clear();
        // 始终基于本地订阅表进行一次匹配，避免本地订阅已生效但全局路由尚未传播时漏投递。
        SubscriptionMatchResult localMatch = subscriptionRegistry.findSubscriptionMatch(topic);
        localSubscribers.addAll(localMatch.getDirectSubscribers());
        buildRemoteNormalTargetPlans(globalMatch, remoteTargetPlans);
        selectSharedDeliveryTargets(localMatch, globalMatch, localSubscribers, remoteTargetPlans);

        deliverToLocalSubscribers(topic, payload, localSubscribers, publishQos);
        if (!remoteTargetPlans.isEmpty()) {
            // payload 在路由链路内按只读使用，跨节点分发无需额外 clone，减少大消息拷贝开销。
            clusterMessageDispatcher.dispatch(topic, payload, publishQos, remoteTargetPlans);
            LOG.fine(() -> "[ROUTE][CLUSTER] topic=" + topic + ", remoteTargets=" + remoteTargetPlans.keySet());
        }
        localSubscribers.clear();
        remoteTargetPlans.clear();
    }

    /**
     * 解析全局路由匹配结果（短 TTL + appliedLogIndex 一致性门控）。
     * 命中条件：
     * 1. 同 topic 存在缓存；
     * 2. 缓存尚未过期；
     * 3. 全局路由 appliedLogIndex 未变化。
     */
    private GlobalSubscriptionMatch resolveGlobalMatch(String topic) {
        if (topic == null || topic.isBlank() || globalSubscriptionRegistry == null) {
            return EMPTY_GLOBAL_MATCH;
        }
        long now = System.currentTimeMillis();
        long currentLogIndex = globalSubscriptionRegistry.appliedLogIndex();
        CachedGlobalMatch cached = globalMatchCache.get(topic);
        if (cached != null && cached.matches(currentLogIndex, now)) {
            return cached.match();
        }
        GlobalSubscriptionMatch fresh = globalSubscriptionRegistry.match(topic);
        if (fresh == null) {
            fresh = EMPTY_GLOBAL_MATCH;
        }
        if (cached == null && globalMatchCache.size() >= GLOBAL_MATCH_CACHE_MAX_TOPICS) {
            globalMatchCache.clear();
        }
        globalMatchCache.put(topic, new CachedGlobalMatch(fresh, currentLogIndex, now + GLOBAL_MATCH_CACHE_TTL_MS));
        return fresh;
    }

    private void buildRemoteNormalTargetPlans(
        GlobalSubscriptionMatch globalMatch,
        Map<String, ClusterMessageDispatcher.DispatchTarget> plans
    ) {
        if (globalMatch == null || globalMatch.normalNodes().isEmpty()) {
            return;
        }
        for (String targetNodeId : globalMatch.normalNodes()) {
            if (targetNodeId == null || targetNodeId.isBlank() || targetNodeId.equals(nodeId)) {
                continue;
            }
            plans.put(targetNodeId, ClusterMessageDispatcher.DispatchTarget.normalOnly());
        }
    }



    /**
     * 集群远端消息入口。
     * 远端投递只做本地订阅派发，不再继续向其他节点转发，避免形成环路。
     *
     * @param topic   topic
     * @param payload payload
     */
    public void onClusterPublish(String topic, byte[] payload, int publishQos, boolean includeNormal, Set<String> sharedGroups) {
        if (topic == null || topic.isBlank() || payload == null) {
            return;
        }
        int normalizedQos = publishQos >= MqttQoS.AT_LEAST_ONCE.value()
            ? (publishQos >= MqttQoS.EXACTLY_ONCE.value()
                ? MqttQoS.EXACTLY_ONCE.value()
                : MqttQoS.AT_LEAST_ONCE.value())
            : MqttQoS.AT_MOST_ONCE.value();
        normalizedQos = capByMaxQos(normalizedQos);
        // 路由到本地订阅客户端
        routeLocalOnly(topic, payload, includeNormal, sharedGroups, normalizedQos);
    }

    private void routeLocalOnly(String topic, byte[] payload, boolean includeNormal, Set<String> sharedGroups, int publishQos) {
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
        deliverToLocalSubscribers(topic, payload, subscribers, publishQos);
    }

    /**
     * 本地订阅派发
     * @param topic  topic
     * @param payload  payload
     * @param subscribers  subscribers
     */
    private void deliverToLocalSubscribers(String topic, byte[] payload, Set<String> subscribers, int publishQos) {
        LOG.fine(() -> "[ROUTE][LOCAL] topic=" + topic + ", subscribers=" + subscribers.size());
        DeliveryScratch deliveryScratch = deliveryScratchHolder.get();
        Set<Channel> channelsToFlush = deliveryScratch.channelsToFlush();
        channelsToFlush.clear();
        for (String subscriber : subscribers) {
            Optional<ClientSession> sessionOptional = sessionRegistry.get(subscriber);
            if (sessionOptional.isEmpty()) {
                continue;
            }

            Channel channel = sessionOptional.get().channel();
            if (!channel.isActive()) {
                continue;
            }
            int outboundQos = publishQos >= MqttQoS.EXACTLY_ONCE.value()
                ? MqttQoS.EXACTLY_ONCE.value()
                : (publishQos >= MqttQoS.AT_LEAST_ONCE.value()
                    ? MqttQoS.AT_LEAST_ONCE.value()
                    : MqttQoS.AT_MOST_ONCE.value());
            outboundQos = capByMaxQos(outboundQos);
            if (outboundQos == MqttQoS.AT_MOST_ONCE.value()) {
                channel.write(MqttMessageBuilders.publish()
                        .topicName(topic)
                        .retained(false)
                        .qos(MqttQoS.AT_MOST_ONCE)
                        .payload(Unpooled.wrappedBuffer(payload))
                .build());
                channelsToFlush.add(channel);
                continue;
            }
            int packetId = inflightManager.nextOutboundPacketId(subscriber);
            if (outboundQos == MqttQoS.AT_LEAST_ONCE.value()) {
                channel.write(MqttPacketFactory.buildQos1PublishMessage(topic, payload, packetId, false));
                inflightManager.trackInflightQos1(subscriber, packetId, topic, payload);
                channelsToFlush.add(channel);
                continue;
            }
            channel.write(MqttPacketFactory.buildQos2PublishMessage(topic, payload, packetId, false));
            inflightManager.trackInflightQos2(subscriber, packetId, topic, payload);
            channelsToFlush.add(channel);
        }
        for (Channel channel : channelsToFlush) {
            if (channel != null && channel.isActive()) {
                channel.flush();
            }
        }
        channelsToFlush.clear();
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
        boolean hasLocalCandidates = localCandidates != null && !localCandidates.isEmpty();
        List<String> orderedGlobalNodes = resolveOrderedGlobalNodes(group, globalNodes);
        List<String> nodeCandidates = buildNodeCandidates(orderedGlobalNodes, hasLocalCandidates);
        if (nodeCandidates.isEmpty()) {
            return null;
        }
        AtomicLong cursor = sharedGroupNodeRoundRobin.computeIfAbsent(group, ignored -> new AtomicLong(0));
        int start = Math.floorMod(cursor.getAndIncrement(), nodeCandidates.size());
        for (int i = 0; i < nodeCandidates.size(); i++) {
            String selectedNode = nodeCandidates.get((start + i) % nodeCandidates.size());
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

    /**
     * 获取共享组的全局节点有序快照，避免每条消息重复排序。
     */
    private List<String> resolveOrderedGlobalNodes(String group, Set<String> globalNodes) {
        if (group == null || group.isBlank() || globalNodes == null || globalNodes.isEmpty()) {
            return Collections.emptyList();
        }
        int size = globalNodes.size();
        int hash = globalNodes.hashCode();
        SharedGroupNodeOrderSnapshot snapshot = sharedGroupNodeOrderSnapshots.get(group);
        if (snapshot != null && snapshot.matches(size, hash)) {
            return snapshot.orderedNodes();
        }
        List<String> ordered = new ArrayList<>(globalNodes.size());
        for (String node : globalNodes) {
            if (node != null && !node.isBlank()) {
                ordered.add(node);
            }
        }
        Collections.sort(ordered);
        List<String> immutableOrdered = Collections.unmodifiableList(ordered);
        sharedGroupNodeOrderSnapshots.put(group, new SharedGroupNodeOrderSnapshot(size, hash, immutableOrdered));
        return immutableOrdered;
    }

    /**
     * 组装本次共享投递的候选节点列表：
     * 1. 使用全局有序节点快照；
     * 2. 本地存在候选客户端时，把本节点插入有序位置。
     */
    private List<String> buildNodeCandidates(List<String> orderedGlobalNodes, boolean includeLocalNode) {
        if (!includeLocalNode) {
            return orderedGlobalNodes;
        }
        if (orderedGlobalNodes.isEmpty()) {
            return List.of(nodeId);
        }
        int index = Collections.binarySearch(orderedGlobalNodes, nodeId);
        if (index >= 0) {
            return orderedGlobalNodes;
        }
        int insertPosition = -index - 1;
        List<String> merged = new ArrayList<>(orderedGlobalNodes.size() + 1);
        merged.addAll(orderedGlobalNodes.subList(0, insertPosition));
        merged.add(nodeId);
        merged.addAll(orderedGlobalNodes.subList(insertPosition, orderedGlobalNodes.size()));
        return merged;
    }

    /**
     * 批量回放 retained，减少 SUBSCRIBE 场景的 flush 次数。
     */
    private void replayRetainedBatch(Channel channel, Set<String> topicFilters) {
        if (channel == null || !channel.isActive() || topicFilters == null || topicFilters.isEmpty()) {
            return;
        }
        for (String topicFilter : topicFilters) {
            if (topicFilter == null || topicFilter.isBlank()) {
                continue;
            }
            List<RetainedMessage> retainedMessages = retainedMessageStore.findByTopicFilter(topicFilter);
            for (RetainedMessage retained : retainedMessages) {
                channel.write(MqttMessageBuilders.publish()
                        .topicName(retained.topic())
                        .retained(true)
                        .qos(MqttQoS.AT_MOST_ONCE)
                        .payload(Unpooled.wrappedBuffer(retained.payload()))
                        .build());
            }
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

    private String resolveLoggingClientId(Channel channel, MqttMessage message) {
        String current = currentClientId(channel);
        if (current != null && !current.isBlank()) {
            return current;
        }
        if (message instanceof MqttConnectMessage connectMessage
                && connectMessage.payload() != null
                && connectMessage.payload().clientIdentifier() != null
                && !connectMessage.payload().clientIdentifier().isBlank()) {
            return connectMessage.payload().clientIdentifier();
        }
        return null;
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

    /**
     * 管理端订阅同步防抖，避免高频 SUB/UNSUB 每次都做全量快照与上报。
     */
    private void scheduleClientSubscriptionsSync(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        long deadline = System.currentTimeMillis() + ADMIN_SUBSCRIPTIONS_SYNC_DEBOUNCE_MS;
        adminSubscriptionsSyncDeadlineByClient.put(clientId, deadline);
        try {
            maintenanceExecutor.schedule(
                    () -> trySyncClientSubscriptionsIfDue(clientId, deadline),
                    ADMIN_SUBSCRIPTIONS_SYNC_DEBOUNCE_MS,
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException ignored) {
        }
    }

    private void trySyncClientSubscriptionsIfDue(String clientId, long expectedDeadline) {
        Long latest = adminSubscriptionsSyncDeadlineByClient.get(clientId);
        if (latest == null || latest != expectedDeadline) {
            return;
        }
        adminSubscriptionsSyncDeadlineByClient.remove(clientId, latest);
        syncClientSubscriptionsForAdmin(clientId);
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
        if (!bridgeEnabled) {
            return false;
        }
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
        Optional<ClientSession> session = clientId == null ? Optional.empty() : sessionRegistry.get(clientId);
        if (session.map(ClientSession::superuser).orElse(false)) {
            return false;
        }
        String username = session.map(ClientSession::username).orElse(null);
        return !aclAuthorizer.isAllowed(new AclRequest(clientId, username, topic, action));
    }

    private void applyGlobalRegisterAfterLocal(String topicFilter) {
        if (globalSubscriptionRegistry == null || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        String normalized = SharedSubscription.normalizeTopicFilter(topicFilter);
        String group = shared == null ? null : shared.group();
        submitMetadataCommand(new MetadataCommand(
            SUBSCRIPTION_NAMESPACE,
            OP_REGISTER,
            normalized,
            group,
            nodeId
        ), "global-register topicFilter=" + topicFilter);
    }

    /**
     * SUBSCRIBE 报文内首次订阅批量异步提交全局路由，避免阻塞 I/O 线程。
     */
    private void submitGlobalRegisterBatchAsync(List<String> topicFilters) {
        if (topicFilters == null || topicFilters.isEmpty()) {
            return;
        }
        List<String> snapshot = new ArrayList<>(topicFilters);
        try {
            maintenanceExecutor.execute(() -> {
                for (String topicFilter : snapshot) {
                    applyGlobalRegisterAfterLocal(topicFilter);
                }
            });
        } catch (RejectedExecutionException ignored) {
        }
    }

    /**
     * UNSUBSCRIBE 报文内本节点最后引用批量异步提交全局路由删除，避免阻塞 I/O 线程。
     */
    private void submitGlobalUnregisterBatchAsync(Set<String> topicFilters) {
        if (topicFilters == null || topicFilters.isEmpty()) {
            return;
        }
        List<String> snapshot = new ArrayList<>(topicFilters);
        try {
            maintenanceExecutor.execute(() -> {
                for (String topicFilter : snapshot) {
                    applyGlobalUnregisterAfterLocal(topicFilter);
                }
            });
        } catch (RejectedExecutionException ignored) {
        }
    }

    private void applyGlobalUnregisterAfterLocal(String topicFilter) {
        if (globalSubscriptionRegistry == null || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        String normalized = SharedSubscription.normalizeTopicFilter(topicFilter);
        String group = shared == null ? null : shared.group();
        submitMetadataCommand(new MetadataCommand(
            SUBSCRIPTION_NAMESPACE,
            OP_UNREGISTER,
            normalized,
            group,
            nodeId
        ), "global-unregister topicFilter=" + topicFilter);
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
        submitMetadataCommand(new MetadataCommand(
            SESSION_NAMESPACE,
            OP_ONLINE,
            clientId,
            String.valueOf(Math.max(0L, connectedAtMs)),
            nodeId
        ), "session-online clientId=" + clientId);
    }

    private void applyGlobalClientAuthCacheEvictAfterDisconnect(String clientId, String username) {
        if ((clientId == null || clientId.isBlank()) && (username == null || username.isBlank())) {
            return;
        }
        submitMetadataCommand(new MetadataCommand(
            SESSION_NAMESPACE,
            OP_AUTH_CACHE_EVICT,
            clientId == null ? "" : clientId,
            username == null ? "" : username,
            nodeId
        ), "auth-cache-evict clientId=" + clientId);
    }

    public void shutdown() {
        adminReporter.shutdown();
        maintenanceExecutor.shutdown();
        try {
            if (!maintenanceExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                maintenanceExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            maintenanceExecutor.shutdownNow();
        } finally {
            inflightManager.close();
        }
    }


    private void runPeriodicMaintenance() {
        try {
            inflightManager.retryInflight(sessionRegistry);
        } catch (Exception exception) {
            LOG.warning("[INFLIGHT] retry task failed: " + exception.getMessage());
        }
        try {
            retainedCommandReplicator.retryPendingCommands();
        } catch (Exception exception) {
            LOG.warning("[RETAINED] pending command retry task failed: " + exception.getMessage());
        }
        try {
            retryPendingMetadataCommands();
        } catch (Exception exception) {
            LOG.warning("[CLUSTER] metadata pending command retry task failed: " + exception.getMessage());
        }
        try {
            cleanupRateLimitState();
        } catch (Exception exception) {
            LOG.warning("[RATE_LIMIT] cleanup task failed: " + exception.getMessage());
        }
    }

    /**
     * 提交元数据命令；失败时写入待重试队列，由周期任务异步重试。
     * 这里不阻塞主协议线程，避免 SUB/UNSUB/CONNECT 处理链路被远端抖动拖慢。
     */
    private void submitMetadataCommand(MetadataCommand command, String context) {
        String retryKey = metadataRetryKey(command);
        long committedIndex = metadataCommandGateway.submit(command);
        if (committedIndex >= 0) {
            pendingMetadataCommands.remove(retryKey);
            return;
        }
        pendingMetadataCommands.compute(retryKey, (key, previous) -> {
            int attempts = previous == null ? 1 : previous.attempts + 1;
            long nextRetryAt = System.currentTimeMillis() + METADATA_COMMAND_RETRY_INTERVAL_MS;
            return new PendingMetadataCommand(command, attempts, nextRetryAt);
        });
        LOG.warning(() -> "[CLUSTER] metadata submit failed, command queued for retry, context=" + context);
    }

    /**
     * 周期重试元数据命令，成功后从待重试队列移除。
     */
    private void retryPendingMetadataCommands() {
        if (pendingMetadataCommands.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        pendingMetadataCommands.forEach((retryKey, pending) -> {
            if (pending == null || pending.nextRetryAt > now) {
                return;
            }
            long committedIndex = metadataCommandGateway.submit(pending.command);
            if (committedIndex >= 0) {
                pendingMetadataCommands.remove(retryKey, pending);
                return;
            }
            PendingMetadataCommand nextPending = new PendingMetadataCommand(
                pending.command,
                pending.attempts + 1,
                System.currentTimeMillis() + METADATA_COMMAND_RETRY_INTERVAL_MS
            );
            pendingMetadataCommands.replace(retryKey, pending, nextPending);
        });
    }

    /**
     * 命令去重键：
     * 1. 订阅命令按 namespace + key(topic) + value(group) + sourceNode 去重，后写覆盖前写。
     * 2. 会话命令按 namespace + key(clientId) + sourceNode 去重，保留最新 online 事件。
     */
    private static String metadataRetryKey(MetadataCommand command) {
        if (command == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder(128);
        builder.append(Objects.toString(command.namespace(), "")).append('|')
            .append(Objects.toString(command.key(), "")).append('|')
            .append(Objects.toString(command.sourceNodeId(), ""));
        if (SUBSCRIPTION_NAMESPACE.equals(command.namespace())) {
            builder.append('|').append(Objects.toString(command.value(), ""));
        }
        return builder.toString();
    }

    /**
     * 元数据命令待重试快照。
     */
    private static final class PendingMetadataCommand {
        private final MetadataCommand command;
        private final int attempts;
        private final long nextRetryAt;

        private PendingMetadataCommand(MetadataCommand command, int attempts, long nextRetryAt) {
            this.command = command;
            this.attempts = attempts;
            this.nextRetryAt = nextRetryAt;
        }
    }

    /**
     * 共享组节点有序快照。
     */
    private record SharedGroupNodeOrderSnapshot(
        int size,
        int hash,
        List<String> orderedNodes
    ) {
        private boolean matches(int currentSize, int currentHash) {
            return this.size == currentSize && this.hash == currentHash;
        }
    }

    /**
     * topic -> 全局匹配缓存快照。
     */
    private record CachedGlobalMatch(
        GlobalSubscriptionMatch match,
        long appliedLogIndex,
        long expireAtMs
    ) {
        private boolean matches(long currentLogIndex, long now) {
            return this.appliedLogIndex == currentLogIndex && this.expireAtMs > now;
        }
    }

    /**
     * 路由热路径复用容器，减少每条 PUBLISH 的临时对象分配。
     */
    private static final class RouteScratch {
        private final Set<String> localSubscribers = new LinkedHashSet<>();
        private final Map<String, ClusterMessageDispatcher.DispatchTarget> remoteTargetPlans = new HashMap<>();

        private Set<String> localSubscribers() {
            return localSubscribers;
        }

        private Map<String, ClusterMessageDispatcher.DispatchTarget> remoteTargetPlans() {
            return remoteTargetPlans;
        }
    }

    /**
     * 本地投递热路径复用容器，避免每条消息都创建 flush 集合。
     */
    private static final class DeliveryScratch {
        private final Set<Channel> channelsToFlush = new LinkedHashSet<>();

        private Set<Channel> channelsToFlush() {
            return channelsToFlush;
        }
    }

    private boolean isPublishRateLimited(ChannelHandlerContext ctx, String clientId, int qos, int packetId, String topic) {
        long now = System.currentTimeMillis();
        String limitedType = rateLimiter.checkPublish(clientId, resolveClientIp(ctx.channel()), now);
        if (limitedType != null) {
            handleRateLimitedPublish(ctx, clientId, limitedType, qos, packetId, topic);
            return true;
        }
        return false;
    }

    private static int normalizeQos(int qos) {
        if (qos <= MqttQoS.AT_MOST_ONCE.value()) {
            return MqttQoS.AT_MOST_ONCE.value();
        }
        if (qos >= MqttQoS.EXACTLY_ONCE.value()) {
            return MqttQoS.EXACTLY_ONCE.value();
        }
        return qos;
    }

    private static int normalizeMaxQos(int qos) {
        return normalizeQos(qos);
    }

    private int capByMaxQos(int qos) {
        return Math.min(normalizeQos(qos), maxAllowedQos);
    }

    private void handleRateLimitedPublish(
            ChannelHandlerContext ctx,
            String clientId,
            String type,
            int qos,
            int packetId,
            String topic
    ) {
        LOG.warning(() -> "[RATE_LIMIT] publish throttled type=" + type
                + ", clientId=" + clientId + ", topic=" + topic + ", qos=" + qos);
        if (qos == 1) {
            ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(packetId).build());
            return;
        }
        if (qos == 2) {
            ctx.writeAndFlush(MqttPacketFactory.buildPubRecMessage(packetId));
        }
    }

    private void cleanupRateLimitState() {
        rateLimiter.cleanupIfDue(System.currentTimeMillis());
    }

    private boolean isConnectRateLimited(ChannelHandlerContext ctx, boolean mqtt5) {
        String limitedType = rateLimiter.checkConnect(resolveClientIp(ctx.channel()), System.currentTimeMillis());
        if (limitedType != null) {
            handleRateLimitedConnect(ctx, limitedType, mqtt5);
            return true;
        }
        return false;
    }

    private void handleRateLimitedConnect(ChannelHandlerContext ctx, String type, boolean mqtt5) {
        LOG.warning(() -> "[RATE_LIMIT] connect throttled type=" + type + ", remote=" + ctx.channel().remoteAddress());
        rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE, mqtt5);
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

}
