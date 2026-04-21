package com.jmqx.broker.core;

import com.jmqx.admin.AdminReporter;
import com.jmqx.acl.AclAction;
import com.jmqx.acl.AclDecision;
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
import io.netty.channel.ChannelFuture;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Core MQTT message handler.
 *
 * <p>This class owns the broker hot path for CONNECT, SUBSCRIBE, PUBLISH, inflight
 * state transitions, routing, and a small amount of cluster synchronization glue.
 * The implementation intentionally keeps the protocol flow explicit so each stage
 * can be read from top to bottom.</p>
 */
public class MqttBrokerMessageHandler implements BrokerMessageHandler {
    private static final Logger LOG = Logger.getLogger(MqttBrokerMessageHandler.class.getName());
    private static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("jmqx.clientId");
    private static final AttributeKey<Boolean> CLEAN_START = AttributeKey.valueOf("jmqx.cleanStart");
    private static final AttributeKey<String> WS_USERNAME = AttributeKey.valueOf("jmqx.ws.username");
    private static final AttributeKey<String> CONNECTION_TYPE = AttributeKey.valueOf("jmqx.connectionType");
    private static final AttributeKey<Boolean> GRACEFUL_DISCONNECT = AttributeKey.valueOf("jmqx.gracefulDisconnect");
    private static final AttributeKey<WillMessage> WILL_MESSAGE = AttributeKey.valueOf("jmqx.willMessage");
    private static final AttributeKey<Boolean> CONNECT_IN_PROGRESS = AttributeKey.valueOf("jmqx.connectInProgress");
    private static final AttributeKey<ChannelTaskSequencer> PUBLISH_TASK_SEQUENCER =
        AttributeKey.valueOf("jmqx.publishTaskSequencer");

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
    private final ExecutorService connectAuthExecutor;
    private final ExecutorService publishAclExecutor;
    private final SecurityPipelineMetrics securityPipelineMetrics;
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
        int authWorkerCount = normalizedWorkerCount("auth");
        this.connectAuthExecutor = Executors.newFixedThreadPool(authWorkerCount, runnable -> {
            Thread thread = new Thread(runnable, "jmqx-connect-auth-" + runnable.hashCode());
            thread.setDaemon(true);
            return thread;
        });
        int aclWorkerCount = normalizedWorkerCount("acl");
        this.publishAclExecutor = Executors.newFixedThreadPool(aclWorkerCount, runnable -> {
            Thread thread = new Thread(runnable, "jmqx-publish-acl-" + runnable.hashCode());
            thread.setDaemon(true);
            return thread;
        });
        this.securityPipelineMetrics = new SecurityPipelineMetrics(LOG);
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

    public SecurityPipelineMetrics.Snapshot securityPipelineMetricsSnapshot() {
        return securityPipelineMetrics.snapshot();
    }

    @Override
    public void onMessage(ChannelHandlerContext ctx, MqttMessage message) {
        // Close immediately on decode failure to avoid keeping an invalid connection alive.
        if (message.decoderResult().isFailure()) {
            LOG.warning(() -> "[PROTO] decode failed, remote=" + ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // Dispatch by MQTT message type.
        MqttMessageType messageType = message.fixedHeader().messageType();
        if (shouldRejectWhileConnectPending(ctx.channel(), messageType)) {
            LOG.warning(() -> "[CONNECT] closing connection because protocol message arrived before CONNACK, remote="
                + ctx.channel().remoteAddress() + ", messageType=" + messageType);
            ctx.close();
            return;
        }
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

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {
        if (rejectDuplicateConnectWhilePending(ctx)) {
            return;
        }
        MqttVersion mqttVersion = validateConnectVersion(ctx, message);
        if (mqttVersion == null) {
            return;
        }
        boolean mqtt5 = mqttVersion == MqttVersion.MQTT_5;
        if (isConnectRateLimited(ctx, mqtt5)) {
            return;
        }
        String clientId = validateConnectClientId(ctx, message, mqtt5);
        if (clientId == null) {
            return;
        }
        PendingConnectContext connectContext = buildPendingConnectContext(ctx, message, mqttVersion, mqtt5, clientId);
        if (connectContext == null) {
            return;
        }
        if (rejectBlacklistedConnect(ctx, connectContext.clientId(), connectContext.clientIp(), mqtt5)) {
            return;
        }
        startConnectAuthentication(ctx, connectContext);
    }

    private boolean rejectDuplicateConnectWhilePending(ChannelHandlerContext ctx) {
        if (!Boolean.TRUE.equals(ctx.channel().attr(CONNECT_IN_PROGRESS).get())) {
            return false;
        }
        LOG.warning(() -> "[CONNECT] duplicate CONNECT while authentication in progress, remote=" + ctx.channel().remoteAddress());
        ctx.close();
        return true;
    }

    private MqttVersion validateConnectVersion(ChannelHandlerContext ctx, MqttConnectMessage message) {
        MqttVersion mqttVersion = resolveMqttVersion(message);
        if (mqttVersion == null) {
            LOG.warning(() -> "[CONNECT] rejected unknown protocol version, remote=" + ctx.channel().remoteAddress()
                + ", version=" + message.variableHeader().version());
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false);
            return null;
        }
        if (mqttVersion == MqttVersion.MQTT_5
            || mqttVersion == MqttVersion.MQTT_3_1
            || mqttVersion == MqttVersion.MQTT_3_1_1) {
            return mqttVersion;
        }
        LOG.warning(() -> "[CONNECT] rejected unsupported protocol version, remote=" + ctx.channel().remoteAddress()
            + ", version=" + message.variableHeader().version());
        rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false);
        return null;
    }

    private String validateConnectClientId(ChannelHandlerContext ctx, MqttConnectMessage message, boolean mqtt5) {
        String clientId = message.payload().clientIdentifier();
        if (clientId != null && !clientId.isBlank()) {
            return clientId;
        }
        LOG.warning(() -> "[CONNECT] rejected empty clientId, remote=" + ctx.channel().remoteAddress());
        rejectConnection(
            ctx,
            mqtt5
                ? MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID
                : MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
            mqtt5
        );
        return null;
    }

    private PendingConnectContext buildPendingConnectContext(
        ChannelHandlerContext ctx,
        MqttConnectMessage message,
        MqttVersion mqttVersion,
        boolean mqtt5,
        String clientId
    ) {
        if (rejectOversizeWillPayload(ctx, message, clientId, mqtt5)) {
            return null;
        }
        boolean cleanStart = message.variableHeader().isCleanSession();
        return new PendingConnectContext(
            mqttVersion,
            mqtt5,
            clientId,
            resolveConnectUsername(ctx, message, clientId),
            resolveConnectPassword(message),
            resolveClientIp(ctx.channel()),
            resolveConnectionType(ctx.channel()),
            cleanStart,
            mqtt5
                ? resolveSessionExpiryIntervalSeconds(message)
                : (cleanStart ? SESSION_EXPIRY_IMMEDIATE : SESSION_EXPIRY_PERSISTENT),
            Math.max(message.variableHeader().keepAliveTimeSeconds(), 0),
            resolveServiceNodeIp(ctx.channel()),
            buildWillMessage(message),
            System.nanoTime()
        );
    }

    private String resolveConnectUsername(ChannelHandlerContext ctx, MqttConnectMessage message, String clientId) {
        String username = normalize(message.payload().userName());
        if (username == null) {
            username = normalize(ctx.channel().attr(WS_USERNAME).get());
        }
        return username == null ? clientId : username;
    }

    private String resolveConnectPassword(MqttConnectMessage message) {
        byte[] passwordBytes = message.payload().passwordInBytes();
        if (passwordBytes == null) {
            return null;
        }
        return new String(passwordBytes, StandardCharsets.UTF_8);
    }

    private String resolveConnectionType(Channel channel) {
        String connectionType = normalize(channel.attr(CONNECTION_TYPE).get());
        return connectionType == null ? "mqtt" : connectionType;
    }

    private boolean rejectOversizeWillPayload(
        ChannelHandlerContext ctx,
        MqttConnectMessage message,
        String clientId,
        boolean mqtt5
    ) {
        if (!isWillPayloadTooLarge(message)) {
            return false;
        }
        LOG.warning(() -> "[CONNECT] rejected oversize will payload, clientId=" + clientId
            + ", maxWillPayloadBytes=" + maxWillPayloadBytes);
        rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED, mqtt5);
        return true;
    }

    private boolean rejectBlacklistedConnect(ChannelHandlerContext ctx, String clientId, String clientIp, boolean mqtt5) {
        if (!clientBlacklist.isBlocked(clientId, clientIp)) {
            return false;
        }
        LOG.warning(() -> "[CONNECT] blacklisted client rejected, clientId=" + clientId + ", clientIp=" + clientIp);
        rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED, mqtt5);
        return true;
    }

    private void startConnectAuthentication(ChannelHandlerContext ctx, PendingConnectContext connectContext) {
        ctx.channel().attr(CONNECT_IN_PROGRESS).set(true);
        authenticateConnectAsync(connectContext).whenComplete((authResult, error) ->
            executeOnEventLoop(ctx, () -> completeConnectAfterAuth(ctx, connectContext, authResult, error))
        );
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
            // ACL authorization
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
            // Update local subscriptions first, then synchronize new global routes.
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
        PreparedPublish preparedPublish = preparePublish(ctx, message);
        if (preparedPublish == null) {
            return;
        }
        publishSequencer(ctx.channel()).submit(() ->
            authorizePublishAsync(preparedPublish.session(), preparedPublish.context())
                .handle(PublishAclEvaluation::new)
                .thenCompose(result -> executeOnEventLoop(
                    ctx,
                    () -> completePublishAfterAcl(ctx, preparedPublish.context(), result.decision(), result.error())
                ))
        );
    }

    /**
     * Returns protocol acknowledgements when ACL denies a publish.
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
     * Stores inbound QoS2 PUBLISH and replies with PUBREC.
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
     * Handles the application-level publish path.
     *
     * <p>QoS0 and QoS1 publish directly. QoS2 enters here later when PUBREL arrives.</p>
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

    private PreparedPublish preparePublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null) {
            ctx.close();
            return null;
        }
        String topic = message.variableHeader().topicName();
        int qos = normalizeQos(message.fixedHeader().qosLevel().value());
        int packetId = message.variableHeader().packetId();
        if (rejectInvalidPublishPacketId(ctx, clientId, qos, packetId)) {
            return null;
        }
        if (isPublishRateLimited(ctx, clientId, qos, packetId, topic)) {
            return null;
        }
        ClientSession session = requirePublishSession(ctx, clientId);
        if (session == null) {
            return null;
        }
        PublishContext publishContext = new PublishContext(
            clientId,
            session.username(),
            topic,
            ByteBufUtil.getBytes(message.payload()),
            qos,
            capByMaxQos(qos),
            packetId,
            message.fixedHeader().isRetain(),
            System.nanoTime()
        );
        return new PreparedPublish(session, publishContext);
    }

    private boolean rejectInvalidPublishPacketId(
        ChannelHandlerContext ctx,
        String clientId,
        int qos,
        int packetId
    ) {
        if (qos <= 0 || packetId > 0) {
            return false;
        }
        LOG.warning(() -> "[PUBLISH] invalid packetId clientId=" + clientId + ", qos=" + qos);
        ctx.close();
        return true;
    }

    private ClientSession requirePublishSession(ChannelHandlerContext ctx, String clientId) {
        Optional<ClientSession> sessionOptional = sessionRegistry.get(clientId);
        if (sessionOptional.isPresent()) {
            return sessionOptional.get();
        }
        ctx.close();
        return null;
    }

    private void handlePubAck(ChannelHandlerContext ctx, MqttPubAckMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null || message == null || message.variableHeader() == null) {
            return;
        }
        int packetId = message.variableHeader().messageId();
        inflightManager.onPubAck(clientId, packetId);
    }

    /**
     * QoS2 phase 2: reply with PUBREL after PUBREC and wait for PUBCOMP before clearing state.
     */
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

    /**
     * QoS2 phase 3: process the application publish after PUBREL and then reply with PUBCOMP.
     */
    private void handlePubRel(ChannelHandlerContext ctx, MqttMessage message) {
        String clientId = currentClientId(ctx.channel());
        Integer packetId = MqttPacketFactory.extractPacketId(message);
        if (clientId == null || packetId == null) {
            return;
        }
        BrokerInflightManager.InboundQos2ReleaseDecision decision = inflightManager.onPubRel(clientId, packetId);
        if (decision.shouldProcess()) {
            InboundQos2Publish publish = decision.publish();
            // QoS2 application publish
            processApplicationPublish(
                clientId,
                publish.topic(),
                publish.payload(),
                capByMaxQos(MqttQoS.EXACTLY_ONCE.value()),
                publish.retain()
            );
            inflightManager.markInboundQos2Completed(clientId, packetId);
        }
        ChannelFuture future = ctx.writeAndFlush(MqttPacketFactory.buildPubCompMessage(packetId));
        if (decision.shouldCleanupAfterAck()) {
            future.addListener(writeFuture -> {
                if (writeFuture.isSuccess()) {
                    inflightManager.removeInboundQos2(clientId, packetId);
                }
            });
        }
    }

    /**
     * QoS2 phase 4: clear outbound inflight state after PUBCOMP.
     */
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
     * Publishes an internal system topic, mainly for dashboard and other control-plane consumers.
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
        GlobalSubscriptionMatch globalMatch = resolveGlobalMatch(topic);
        RouteScratch routeScratch = prepareRouteScratch();
        Set<String> localSubscribers = routeScratch.localSubscribers();
        Map<String, ClusterMessageDispatcher.DispatchTarget> remoteTargetPlans = routeScratch.remoteTargetPlans();
        SubscriptionMatchResult localMatch = subscriptionRegistry.findSubscriptionMatch(topic);
        localSubscribers.addAll(localMatch.getDirectSubscribers());
        buildRemoteNormalTargetPlans(globalMatch, remoteTargetPlans);
        selectSharedDeliveryTargets(localMatch, globalMatch, localSubscribers, remoteTargetPlans);
        dispatchRouteResult(topic, payload, publishQos, localSubscribers, remoteTargetPlans);
        clearRouteScratch(routeScratch);
    }

    private RouteScratch prepareRouteScratch() {
        RouteScratch routeScratch = routeScratchHolder.get();
        routeScratch.localSubscribers().clear();
        routeScratch.remoteTargetPlans().clear();
        return routeScratch;
    }

    private void dispatchRouteResult(
        String topic,
        byte[] payload,
        int publishQos,
        Set<String> localSubscribers,
        Map<String, ClusterMessageDispatcher.DispatchTarget> remoteTargetPlans
    ) {
        deliverToLocalSubscribers(topic, payload, localSubscribers, publishQos);
        if (remoteTargetPlans.isEmpty()) {
            return;
        }
        clusterMessageDispatcher.dispatch(topic, payload, publishQos, remoteTargetPlans);
        LOG.fine(() -> "[ROUTE][CLUSTER] topic=" + topic + ", remoteTargets=" + remoteTargetPlans.keySet());
    }

    private void clearRouteScratch(RouteScratch routeScratch) {
        routeScratch.localSubscribers().clear();
        routeScratch.remoteTargetPlans().clear();
    }

    /**
     * Resolves the global routing snapshot with a short TTL cache guarded by the
     * applied log index.
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
     * Cluster entrypoint for messages already dispatched by another node.
     *
     * <p>Remote deliveries only fan out to local subscribers and never dispatch
     * again, which prevents routing loops.</p>
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
        routeLocalOnly(topic, payload, includeNormal, sharedGroups, normalizedQos);
    }

    private void routeLocalOnly(String topic, byte[] payload, boolean includeNormal, Set<String> sharedGroups, int publishQos) {
        if (!includeNormal && (sharedGroups == null || sharedGroups.isEmpty())) {
            return;
        }
        SubscriptionMatchResult matchResult = subscriptionRegistry.findSubscriptionMatch(topic);
        Set<String> subscribers = collectLocalOnlySubscribers(matchResult, includeNormal, sharedGroups);
        deliverToLocalSubscribers(topic, payload, subscribers, publishQos);
    }

    private Set<String> collectLocalOnlySubscribers(
        SubscriptionMatchResult matchResult,
        boolean includeNormal,
        Set<String> sharedGroups
    ) {
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
        return subscribers;
    }

    /**
     * Delivers a routed message to matching local subscribers.
     */
    private void deliverToLocalSubscribers(String topic, byte[] payload, Set<String> subscribers, int publishQos) {
        LOG.fine(() -> "[ROUTE][LOCAL] topic=" + topic + ", subscribers=" + subscribers.size());
        DeliveryScratch deliveryScratch = deliveryScratchHolder.get();
        Set<Channel> channelsToFlush = deliveryScratch.channelsToFlush();
        channelsToFlush.clear();
        for (String subscriber : subscribers) {
            ClientSession session = resolveActiveSubscriberSession(subscriber);
            if (session == null) {
                continue;
            }
            writeLocalDelivery(session.channel(), subscriber, topic, payload, publishQos, channelsToFlush);
        }
        flushDeliveryChannels(channelsToFlush);
    }

    private ClientSession resolveActiveSubscriberSession(String subscriber) {
        Optional<ClientSession> sessionOptional = sessionRegistry.get(subscriber);
        if (sessionOptional.isEmpty()) {
            return null;
        }
        ClientSession session = sessionOptional.get();
        return session.channel().isActive() ? session : null;
    }

    private void writeLocalDelivery(
        Channel channel,
        String subscriber,
        String topic,
        byte[] payload,
        int publishQos,
        Set<Channel> channelsToFlush
    ) {
        int outboundQos = resolveOutboundQos(publishQos);
        if (outboundQos == MqttQoS.AT_MOST_ONCE.value()) {
            channel.write(MqttMessageBuilders.publish()
                .topicName(topic)
                .retained(false)
                .qos(MqttQoS.AT_MOST_ONCE)
                .payload(Unpooled.wrappedBuffer(payload))
                .build());
            channelsToFlush.add(channel);
            return;
        }
        int packetId = inflightManager.nextOutboundPacketId(subscriber);
        if (outboundQos == MqttQoS.AT_LEAST_ONCE.value()) {
            channel.write(MqttPacketFactory.buildQos1PublishMessage(topic, payload, packetId, false));
            inflightManager.trackInflightQos1(subscriber, packetId, topic, payload);
            channelsToFlush.add(channel);
            return;
        }
        inflightManager.trackInflightQos2(subscriber, packetId, topic, payload);
        channel.write(MqttPacketFactory.buildQos2PublishMessage(topic, payload, packetId, false));
        channelsToFlush.add(channel);
    }

    private int resolveOutboundQos(int publishQos) {
        int outboundQos = publishQos >= MqttQoS.EXACTLY_ONCE.value()
            ? MqttQoS.EXACTLY_ONCE.value()
            : (publishQos >= MqttQoS.AT_LEAST_ONCE.value()
                ? MqttQoS.AT_LEAST_ONCE.value()
                : MqttQoS.AT_MOST_ONCE.value());
        return capByMaxQos(outboundQos);
    }

    private void flushDeliveryChannels(Set<Channel> channelsToFlush) {
        for (Channel channel : channelsToFlush) {
            if (channel != null && channel.isActive()) {
                channel.flush();
            }
        }
        channelsToFlush.clear();
    }

    /**
     * Chooses exactly one delivery target per shared group to avoid duplicate
     * delivery within the same group.
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
     * Selects one shared-delivery target from local clients and remote nodes.
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
     * Resolves a stable ordered node snapshot for one shared group.
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
     * Builds the ordered candidate node list for one shared-delivery attempt.
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
     * Replays retained messages in batches to reduce flush frequency during SUBSCRIBE.
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
     * Debounces admin subscription synchronization to avoid sending a full snapshot after every SUB/UNSUB.
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
     * Bridge topic selection rules:
     * 1. When topic filters exist, only bridge matched topics.
     * 2. Without topic filters, bridge all non-dashboard topics.
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
     * Asynchronously submits global registrations for first-time subscriptions.
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
     * Asynchronously submits global unregistrations when the local node removes the last reference.
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
     * Applies the local-online event after CONNECT succeeds so the cluster can enforce one session per clientId.
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
        connectAuthExecutor.shutdown();
        publishAclExecutor.shutdown();
        maintenanceExecutor.shutdown();
        try {
            if (!connectAuthExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                connectAuthExecutor.shutdownNow();
            }
            if (!publishAclExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                publishAclExecutor.shutdownNow();
            }
            if (!maintenanceExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                maintenanceExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            connectAuthExecutor.shutdownNow();
            publishAclExecutor.shutdownNow();
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
     * Submits one metadata command and queues it for retry on failure.
     *
     * <p>The caller stays simple and the periodic retry task owns later retries.</p>
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
     * Periodically retries queued metadata commands and removes successful entries.
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
     * Builds the retry de-duplication key.
     *
     * <p>Subscription commands de-duplicate by namespace + topic + group + source node.
     * Session commands de-duplicate by namespace + clientId + source node.</p>
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

    private CompletableFuture<AuthResult> authenticateConnectAsync(PendingConnectContext connectContext) {
        try {
            return CompletableFuture.supplyAsync(() ->
                clientAuthenticator.authenticateAsync(
                    connectContext.clientId(),
                    connectContext.username(),
                    connectContext.password()
                ),
                connectAuthExecutor
            ).thenCompose(asyncFuture -> asyncFuture);
        } catch (RejectedExecutionException exception) {
            CompletableFuture<AuthResult> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            return future;
        }
    }

    private CompletableFuture<AclDecision> authorizePublishAsync(ClientSession session, PublishContext publishContext) {
        if (session.superuser()) {
            return CompletableFuture.completedFuture(AclDecision.ALLOW);
        }
        AclRequest request = new AclRequest(
            publishContext.clientId(),
            publishContext.username(),
            publishContext.topic(),
            AclAction.PUBLISH
        );
        try {
            return CompletableFuture.supplyAsync(() -> aclAuthorizer.authorizeAsync(request), publishAclExecutor)
                .thenCompose(asyncFuture -> asyncFuture);
        } catch (RejectedExecutionException exception) {
            CompletableFuture<AclDecision> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            return future;
        }
    }

    private void completeConnectAfterAuth(
        ChannelHandlerContext ctx,
        PendingConnectContext connectContext,
        AuthResult authResult,
        Throwable error
    ) {
        ctx.channel().attr(CONNECT_IN_PROGRESS).set(false);
        if (!ctx.channel().isActive()) {
            return;
        }
        long authDurationNanos = System.nanoTime() - connectContext.authStartedAtNanos();
        if (handleFailedConnectAuthentication(ctx, connectContext, authResult, error, authDurationNanos)) {
            return;
        }
        finalizeSuccessfulConnect(ctx, connectContext, authResult, authDurationNanos);
    }

    private void completePublishAfterAcl(
        ChannelHandlerContext ctx,
        PublishContext publishContext,
        AclDecision decision,
        Throwable error
    ) {
        if (!ctx.channel().isActive()) {
            return;
        }
        long aclDurationNanos = System.nanoTime() - publishContext.aclStartedAtNanos();
        if (handlePublishAclError(ctx, publishContext, error, aclDurationNanos)) {
            return;
        }
        if (handlePublishAclDeny(ctx, publishContext, decision, aclDurationNanos)) {
            return;
        }
        completeAuthorizedPublish(ctx, publishContext, aclDurationNanos);
    }

    private boolean handleFailedConnectAuthentication(
        ChannelHandlerContext ctx,
        PendingConnectContext connectContext,
        AuthResult authResult,
        Throwable error,
        long authDurationNanos
    ) {
        if (error == null && authResult != null && authResult.decision() == AuthDecision.ALLOW) {
            return false;
        }
        if (error != null) {
            securityPipelineMetrics.recordConnectAuthError(authDurationNanos, connectContext.clientId(), error);
            LOG.log(Level.WARNING, "[CONNECT] async auth failed clientId=" + connectContext.clientId(), error);
        } else {
            securityPipelineMetrics.recordConnectAuthFailure(authDurationNanos, connectContext.clientId());
            LOG.warning("[CONNECT] auth failed clientId=" + connectContext.clientId()
                + ", username=" + connectContext.username());
        }
        rejectConnection(
            ctx,
            connectContext.mqtt5()
                ? MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD
                : MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
            connectContext.mqtt5()
        );
        return true;
    }

    private void finalizeSuccessfulConnect(
        ChannelHandlerContext ctx,
        PendingConnectContext connectContext,
        AuthResult authResult,
        long authDurationNanos
    ) {
        securityPipelineMetrics.recordConnectAuthSuccess(authDurationNanos, connectContext.clientId());
        boolean sessionPresent = hasPersistedSession(connectContext);
        prepareConnectedChannel(ctx.channel(), connectContext);
        writeSuccessfulConnAck(ctx, connectContext, sessionPresent);
        inflightManager.restoreInflightState(connectContext.clientId(), sessionPresent, ctx.channel());
        Instant connectedAt = Instant.now();
        registerConnectedSession(ctx.channel(), connectContext, authResult, connectedAt);
        publishConnectedLifecycleEvents(connectContext);
        adminReporter.upsertClientSession(
            connectContext.clientId(),
            nodeId,
            connectContext.clientIp(),
            connectContext.keepAliveSeconds(),
            connectContext.connectionType(),
            connectContext.username(),
            connectedAt.toEpochMilli()
        );
        scheduleClientSubscriptionsSync(connectContext.clientId());
        logAcceptedConnect(connectContext);
    }

    private boolean hasPersistedSession(PendingConnectContext connectContext) {
        return !connectContext.cleanStart()
            && inflightManager.hasPersistedSessionState(connectContext.clientId(), subscriptionRegistry);
    }

    private void prepareConnectedChannel(Channel channel, PendingConnectContext connectContext) {
        channel.attr(CLIENT_ID).set(connectContext.clientId());
        channel.attr(CLEAN_START).set(connectContext.cleanStart());
        channel.attr(GRACEFUL_DISCONNECT).set(false);
        applyWillMessageState(channel, connectContext);
    }

    private void applyWillMessageState(Channel channel, PendingConnectContext connectContext) {
        if (connectContext.willMessage() == null) {
            channel.attr(WILL_MESSAGE).set(null);
            willMessageStore.remove(connectContext.clientId());
            return;
        }
        if (willPersistenceEnabled) {
            channel.attr(WILL_MESSAGE).set(null);
            willMessageStore.save(connectContext.clientId(), connectContext.willMessage());
            return;
        }
        channel.attr(WILL_MESSAGE).set(connectContext.willMessage());
    }

    private void writeSuccessfulConnAck(
        ChannelHandlerContext ctx,
        PendingConnectContext connectContext,
        boolean sessionPresent
    ) {
        if (connectContext.mqtt5()) {
            ctx.writeAndFlush(MqttMessageBuilders.connAck()
                .sessionPresent(sessionPresent)
                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .properties(buildConnAckProperties(connectContext.sessionExpirySeconds()))
                .build());
            return;
        }
        ctx.writeAndFlush(MqttMessageBuilders.connAck()
            .sessionPresent(sessionPresent)
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build());
    }

    private void registerConnectedSession(
        Channel channel,
        PendingConnectContext connectContext,
        AuthResult authResult,
        Instant connectedAt
    ) {
        sessionRegistry.register(new ClientSession(
            connectContext.clientId(),
            channel,
            connectContext.connectionType(),
            connectContext.cleanStart(),
            connectContext.sessionExpirySeconds(),
            connectContext.username(),
            connectContext.serviceNodeIp(),
            connectContext.keepAliveSeconds(),
            authResult.superuser(),
            connectedAt
        ));
        applyGlobalClientOnlineAfterLocalConnect(connectContext.clientId(), connectedAt.toEpochMilli());
    }

    private void publishConnectedLifecycleEvents(PendingConnectContext connectContext) {
        publishClientLifecycleEvent(
            TOPIC_CLIENT_CONNECTED,
            dashboardClusterId,
            nodeId,
            connectContext.clientId(),
            connectContext.clientIp(),
            connectContext.username(),
            connectContext.connectionType(),
            connectContext.serviceNodeIp(),
            connectContext.keepAliveSeconds(),
            "connected"
        );
        publishClientLifecycleEvent(
            dashboardTopic("client/connected"),
            dashboardClusterId,
            nodeId,
            connectContext.clientId(),
            connectContext.clientIp(),
            connectContext.username(),
            connectContext.connectionType(),
            connectContext.serviceNodeIp(),
            connectContext.keepAliveSeconds(),
            "connected"
        );
    }

    private void logAcceptedConnect(PendingConnectContext connectContext) {
        if (!LOG.isLoggable(Level.FINE)) {
            return;
        }
        LOG.fine("[CONNECT] accepted clientId=" + connectContext.clientId()
            + ", connectionType=" + connectContext.connectionType()
            + ", username=" + connectContext.username()
            + ", serviceNodeIp=" + connectContext.serviceNodeIp()
            + ", protocol=" + connectContext.mqttVersion()
            + ", cleanStart=" + connectContext.cleanStart()
            + ", sessionExpirySeconds=" + connectContext.sessionExpirySeconds()
            + ", keepAliveSeconds=" + connectContext.keepAliveSeconds());
    }

    private boolean handlePublishAclError(
        ChannelHandlerContext ctx,
        PublishContext publishContext,
        Throwable error,
        long aclDurationNanos
    ) {
        if (error == null) {
            return false;
        }
        securityPipelineMetrics.recordPublishAclError(
            aclDurationNanos,
            publishContext.clientId(),
            publishContext.topic(),
            error
        );
        LOG.log(Level.WARNING, "[ACL] async publish acl failed clientId=" + publishContext.clientId()
            + ", topic=" + publishContext.topic(), error);
        handleDeniedPublish(
            ctx,
            publishContext.clientId(),
            publishContext.topic(),
            publishContext.qos(),
            publishContext.packetId()
        );
        return true;
    }

    private boolean handlePublishAclDeny(
        ChannelHandlerContext ctx,
        PublishContext publishContext,
        AclDecision decision,
        long aclDurationNanos
    ) {
        if (decision == AclDecision.ALLOW) {
            return false;
        }
        securityPipelineMetrics.recordPublishAclDeny(
            aclDurationNanos,
            publishContext.clientId(),
            publishContext.topic()
        );
        handleDeniedPublish(
            ctx,
            publishContext.clientId(),
            publishContext.topic(),
            publishContext.qos(),
            publishContext.packetId()
        );
        return true;
    }

    private void completeAuthorizedPublish(
        ChannelHandlerContext ctx,
        PublishContext publishContext,
        long aclDurationNanos
    ) {
        securityPipelineMetrics.recordPublishAclAllow(
            aclDurationNanos,
            publishContext.clientId(),
            publishContext.topic()
        );
        if (publishContext.qos() == 2) {
            handleInboundQos2Publish(
                ctx,
                publishContext.clientId(),
                publishContext.topic(),
                publishContext.payload(),
                publishContext.packetId(),
                publishContext.retain()
            );
            return;
        }
        processApplicationPublish(
            publishContext.clientId(),
            publishContext.topic(),
            publishContext.payload(),
            publishContext.effectiveQos(),
            publishContext.retain()
        );
        logAcceptedPublish(publishContext);
        if (publishContext.qos() == 1) {
            ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(publishContext.packetId()).build());
        }
    }

    private void logAcceptedPublish(PublishContext publishContext) {
        LOG.fine(() -> "[PUBLISH] clientId=" + publishContext.clientId()
            + ", topic=" + publishContext.topic()
            + ", qos=" + publishContext.qos()
            + ", effectiveQos=" + publishContext.effectiveQos()
            + ", retain=" + publishContext.retain()
            + ", bytes=" + publishContext.payload().length);
    }

    private CompletableFuture<Void> executeOnEventLoop(ChannelHandlerContext ctx, Runnable task) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            ctx.executor().execute(() -> {
                try {
                    task.run();
                    future.complete(null);
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (RejectedExecutionException exception) {
            future.completeExceptionally(exception);
        }
        return future;
    }

    private ChannelTaskSequencer publishSequencer(Channel channel) {
        ChannelTaskSequencer existing = channel.attr(PUBLISH_TASK_SEQUENCER).get();
        if (existing != null) {
            return existing;
        }
        ChannelTaskSequencer created = new ChannelTaskSequencer();
        ChannelTaskSequencer raced = channel.attr(PUBLISH_TASK_SEQUENCER).setIfAbsent(created);
        return raced == null ? created : raced;
    }

    private boolean shouldRejectWhileConnectPending(Channel channel, MqttMessageType messageType) {
        if (messageType == MqttMessageType.CONNECT) {
            return false;
        }
        return currentClientId(channel) == null && Boolean.TRUE.equals(channel.attr(CONNECT_IN_PROGRESS).get());
    }

    private static int normalizedWorkerCount(String category) {
        int processors = Runtime.getRuntime().availableProcessors();
        return Math.max(2, Math.min(16, processors));
    }

    private record PendingConnectContext(
        MqttVersion mqttVersion,
        boolean mqtt5,
        String clientId,
        String username,
        String password,
        String clientIp,
        String connectionType,
        boolean cleanStart,
        long sessionExpirySeconds,
        int keepAliveSeconds,
        String serviceNodeIp,
        WillMessage willMessage,
        long authStartedAtNanos
    ) {
    }

    private record PublishContext(
        String clientId,
        String username,
        String topic,
        byte[] payload,
        int qos,
        int effectiveQos,
        int packetId,
        boolean retain,
        long aclStartedAtNanos
    ) {
    }

    private record PublishAclEvaluation(
        AclDecision decision,
        Throwable error
    ) {
    }

    private record PreparedPublish(
        ClientSession session,
        PublishContext context
    ) {
    }

    private static final class ChannelTaskSequencer {
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

        synchronized void submit(Supplier<CompletableFuture<Void>> taskSupplier) {
            tail = tail.handle((ignored, error) -> null)
                .thenCompose(ignored -> {
                    try {
                        CompletableFuture<Void> future = taskSupplier.get();
                        return future == null ? CompletableFuture.completedFuture(null) : future;
                    } catch (Exception exception) {
                        CompletableFuture<Void> failed = new CompletableFuture<>();
                        failed.completeExceptionally(exception);
                        return failed;
                    }
                })
                .exceptionally(error -> null);
        }
    }

    /**
     * Retry snapshot for one metadata command.
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
     * Ordered global-node snapshot for one shared group.
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
     * Cached global match snapshot for one topic.
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
     * Reusable routing scratch space for the publish hot path.
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
     * Reusable flush set for local delivery.
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
