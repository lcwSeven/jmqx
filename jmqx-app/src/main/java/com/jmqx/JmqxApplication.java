package com.jmqx;

import com.jmqx.acl.AclAuthorizerFactory;
import com.jmqx.acl.AclProperties;
import com.jmqx.acl.ReloadableAclAuthorizer;
import com.jmqx.admin.AdminReporter;
import com.jmqx.admin.HttpAdminReporter;
import com.jmqx.admin.embedded.*;
import com.jmqx.auth.AuthProperties;
import com.jmqx.auth.AuthProviderFactory;
import com.jmqx.auth.AuthRequest;
import com.jmqx.auth.ReloadableAuthProvider;
import com.jmqx.bridge.BridgeProperties;
import com.jmqx.bridge.MessageBridge;
import com.jmqx.bridge.MessageBridgeFactory;
import com.jmqx.bridge.ReloadableMessageBridge;
import com.jmqx.broker.core.AsyncClusterMessageDispatcher;
import com.jmqx.broker.core.ClusterMessageDispatcher;
import com.jmqx.broker.core.MqttBrokerMessageHandler;
import com.jmqx.broker.core.SecurityPipelineMetrics;
import com.jmqx.broker.ratelimit.BrokerRateLimitConfig;
import com.jmqx.cluster.*;
import com.jmqx.cluster.core.MetadataSnapshot;
import com.jmqx.cluster.core.SofaJraftMetadataCommandGateway;
import com.jmqx.cluster.netty.NettyClusterMessageTransport;
import com.jmqx.cluster.netty.NettyMetadataCommandGateway;
import com.jmqx.cluster.netty.NettyMetadataCoreServer;
import com.jmqx.cluster.netty.NettyMetadataReplicantSyncClient;
import com.jmqx.common.BrokerProperties;
import com.jmqx.common.logging.Loggers;
import com.jmqx.common.logging.LoggingBootstrap;
import com.jmqx.config.ClusterSettings;
import com.jmqx.config.JmqxConfig;
import com.jmqx.config.JmqxConfigMappers;
import com.jmqx.protocol.AuthResult;
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.protocol.ClientBlacklist;
import com.jmqx.protocol.RuntimeClientBlacklist;
import com.jmqx.router.LocalSubscriptionRegistry;
import com.jmqx.router.SharedSubscriptionManager;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.router.global.DefaultGlobalSubscriptionRegistry;
import com.jmqx.router.global.GlobalSubscriptionRegistry;
import com.jmqx.session.LocalSessionRegistry;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.async.AsyncQos1InflightStore;
import com.jmqx.store.async.AsyncWillMessageStore;
import com.jmqx.store.async.SharedAsyncStoreExecutor;
import com.jmqx.store.qos.Qos1InflightStore;
import com.jmqx.store.qos.Qos2InflightStore;
import com.jmqx.store.retained.RetainedMessageStore;
import com.jmqx.store.retained.RetainedStoreProperties;
import com.jmqx.store.rocksdb.RocksDbQos1InflightStore;
import com.jmqx.store.rocksdb.RocksDbQos2InflightStore;
import com.jmqx.store.rocksdb.RocksDbRetainedMessageStore;
import com.jmqx.store.rocksdb.RocksDbWillMessageStore;
import com.jmqx.store.will.WillMessageStore;
import com.jmqx.transport.ConnectionMetrics;
import com.jmqx.transport.NettyMqttEndpointServer;
import org.slf4j.Logger;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Application bootstrap entry that wires broker runtime components together.
 *
 * <p>The class is intentionally written as a staged startup pipeline:
 * configuration loading, runtime store creation, security setup, metadata
 * runtime creation, cluster dispatch creation, broker creation, and endpoint
 * startup. Keeping these stages separate makes the startup flow easier to
 * follow and maintain.</p>
 */
public class JmqxApplication {
    private static final Logger LOG = Loggers.getLogger(JmqxApplication.class);

    public static void main(String[] args) throws InterruptedException {
        LoggingBootstrap.initialize();
        // Load config files and JVM overrides first.
        JmqxConfig config = JmqxConfig.loadDefault();
        // Build a small immutable startup context instead of keeping many locals in main.
        StartupContext context = buildStartupContext(config);
        // Start the runtime in well-defined phases.
        RuntimeComponents runtimeComponents = startRuntime(context);
        // Register graceful shutdown in reverse dependency order.
        registerShutdownHook(runtimeComponents);
        // Print a startup summary that shows the effective runtime config.
        printStartupSummary(context);
        // Keep the process alive until a shutdown signal arrives.
        Thread.currentThread().join();
    }

    private static StartupContext buildStartupContext(JmqxConfig config) {
        // Load the main feature configs up front so later stages only depend on StartupContext.
        BrokerProperties brokerProperties = JmqxConfigMappers.loadBrokerProperties(config);
        AuthProperties authProperties = JmqxConfigMappers.loadAuthProperties(config);
        AclProperties aclProperties = JmqxConfigMappers.loadAclProperties(config);
        BridgeProperties bridgeProperties = JmqxConfigMappers.loadBridgeProperties(config);
        RetainedStoreProperties retainedStoreProperties = JmqxConfigMappers.loadRetainedStoreProperties(config);
        // QoS1 inflight store path.
        String qos1InflightRocksdbPath = getStringProperty(
                config,
                "jmqx.qos1.inflight.rocksdb.path",
                "data/qos1-inflight-rocksdb"
        );
        // QoS2 inflight store path.
        String qos2InflightRocksdbPath = getStringProperty(
                config,
                "jmqx.qos2.inflight.rocksdb.path",
                "data/qos2-inflight-rocksdb"
        );
        // Will message persistence flag.
        boolean willPersistEnabled = getBooleanProperty(
                config,
                "jmqx.will.persist.enabled",
                true
        );
        // Will message store path.
        String willRocksdbPath = getStringProperty(
                config,
                "jmqx.will.persist.rocksdb.path",
                "data/will-rocksdb"
        );
        AdminSyncSettings adminSyncSettings = loadAdminSyncSettings(config);
        AdminAuthSettings adminAuthSettings = loadAdminAuthSettings(config);
        AdminPanelSettings adminPanelSettings = loadAdminPanelSettings(config);
        int sharedMaxSubscribers = getIntProperty(config, "jmqx.shared.maxSubscribersPerGroup", 1000);
        int sharedSlowThreshold = getIntProperty(config, "jmqx.shared.slowConsumerStrikeThreshold", 3);
        ClusterDispatchAsyncSettings clusterDispatchAsyncSettings = new ClusterDispatchAsyncSettings(
                getBooleanProperty(config, "jmqx.cluster.dispatch.async.enabled", true),
                getIntProperty(config, "jmqx.cluster.dispatch.async.queueCapacity", 20000),
                getIntProperty(config, "jmqx.cluster.dispatch.async.workerCount", 2),
                getIntProperty(config, "jmqx.cluster.dispatch.async.enqueueTimeoutMs", 2)
        );
        StorageAsyncSettings storageAsyncSettings = new StorageAsyncSettings(
                getBooleanProperty(config, "jmqx.storage.async.enabled", true),
                getIntProperty(config, "jmqx.storage.async.queueCapacity", 20000),
                getIntProperty(config, "jmqx.storage.async.workerCount", 2),
                getIntProperty(config, "jmqx.storage.async.enqueueTimeoutMs", 2)
        );
        String nodeId = getStringProperty(config, "jmqx.node.id", "node-1");
        NodeRole nodeRole = NodeRole.from(getStringProperty(config, "jmqx.cluster.role", "core"), NodeRole.CORE);
        Set<String> coreEndpoints = getStringSetProperty(config, "jmqx.cluster.coreEndpoints");
        ClusterRoleProvider clusterRoleProvider = new StaticClusterRoleProvider(nodeRole, nodeId, coreEndpoints);
        SessionRegistry sessionRegistry = new LocalSessionRegistry();
        SubscriptionRegistry subscriptionRegistry = new LocalSubscriptionRegistry();
        GlobalSubscriptionRegistry globalSubscriptionRegistry = new DefaultGlobalSubscriptionRegistry();
        ClusterSettings clusterSettings = JmqxConfigMappers.loadClusterSettings(config);
        return new StartupContext(
                brokerProperties,
                authProperties,
                aclProperties,
                bridgeProperties,
                retainedStoreProperties,
                qos1InflightRocksdbPath,
                qos2InflightRocksdbPath,
                willPersistEnabled,
                willRocksdbPath,
                sharedMaxSubscribers,
                sharedSlowThreshold,
                clusterDispatchAsyncSettings,
                storageAsyncSettings,
                clusterRoleProvider,
                sessionRegistry,
                subscriptionRegistry,
                globalSubscriptionRegistry,
                clusterSettings,
                adminSyncSettings,
                adminAuthSettings,
                adminPanelSettings
        );
    }

    private static RuntimeComponents startRuntime(StartupContext context) throws InterruptedException {
        RuntimeStores stores = buildRuntimeStores(context);
        SharedSubscriptionManager sharedSubscriptionManager = new SharedSubscriptionManager(
                context.sharedMaxSubscribers(),
                context.sharedSlowThreshold()
        );
        SecurityRuntime securityRuntime = buildSecurityRuntime(context);
        BridgeRuntime bridgeRuntime = buildBridgeRuntime(context, securityRuntime.adminStateRepository());
        MetadataRuntime metadataRuntime = startMetadataRuntime(
                context,
                stores.retainedMessageStore(),
                securityRuntime,
                bridgeRuntime
        );
        ClusterDispatchRuntime clusterDispatchRuntime = buildClusterDispatchRuntime(context);
        ConnectionMetrics connectionMetrics = new ConnectionMetrics();
        AdminReporter adminReporter = buildAdminReporter(
                context.adminSyncSettings(),
                securityRuntime.adminAuthRuntime(),
                context.clusterRoleProvider().nodeId(),
                context.sessionRegistry(),
                connectionMetrics
        );
        MqttBrokerMessageHandler brokerMessageHandler = buildBrokerMessageHandler(
                context,
                stores,
                sharedSubscriptionManager,
                securityRuntime,
                bridgeRuntime,
                metadataRuntime,
                clusterDispatchRuntime.dispatcher(),
                connectionMetrics,
                adminReporter
        );
        bridgeRuntime.brokerMessageHandlerRef().set(brokerMessageHandler);
        adminReporter.setSecurityMetricsSupplier(() -> {
            MqttBrokerMessageHandler current = bridgeRuntime.brokerMessageHandlerRef().get();
            return current == null ? null : current.securityPipelineMetricsSnapshot();
        });
        applyRuntimeBridgeConfig(
                context.bridgeProperties(),
                bridgeRuntime.messageBridge(),
                brokerMessageHandler,
                bridgeRuntime.initialBridgeConfig()
        );
        clusterDispatchRuntime.transport().setMessageConsumer(brokerMessageHandler::onClusterPublish);
        clusterDispatchRuntime.transport().start();
        ScheduledExecutorService dashboardPublisher = startDashboardPublisher(
                brokerMessageHandler,
                context.clusterRoleProvider().nodeId(),
                context.clusterRoleProvider().role().name(),
                context.adminSyncSettings().clusterId(),
                context.adminSyncSettings().nodeIp(),
                context.sessionRegistry(),
                connectionMetrics,
                context.adminSyncSettings().dashboardPublishIntervalMs()
        );
        AdminPanelServer adminPanelServer = buildAdminPanelServer(
                context,
                securityRuntime,
                bridgeRuntime,
                metadataRuntime,
                connectionMetrics,
                brokerMessageHandler
        );
        EndpointServers endpointServers = startEndpointServers(
                context.brokerProperties(),
                brokerMessageHandler,
                connectionMetrics
        );
        return new RuntimeComponents(
                context,
                metadataRuntime,
                clusterDispatchRuntime.transport(),
                clusterDispatchRuntime.dispatcherCloser(),
                stores.sharedAsyncStoreExecutor(),
                securityRuntime.authProvider(),
                brokerMessageHandler,
                stores.retainedMessageStore(),
                stores.willMessageStore(),
                bridgeRuntime.messageBridge(),
                endpointServers,
                dashboardPublisher,
                adminPanelServer
        );
    }

    private static RuntimeStores buildRuntimeStores(StartupContext context) {
        RetainedMessageStore retainedMessageStore = buildRetainedMessageStore(context.retainedStoreProperties());
        SharedAsyncStoreExecutor sharedAsyncStoreExecutor = buildSharedAsyncStoreExecutor(context.storageAsyncSettings());
        Qos1InflightStore qos1InflightStore = buildQos1InflightStore(context, sharedAsyncStoreExecutor);
        Qos2InflightStore qos2InflightStore = buildQos2InflightStore(context, sharedAsyncStoreExecutor);
        WillMessageStore willMessageStore = buildWillMessageStore(context, sharedAsyncStoreExecutor);
        return new RuntimeStores(
                retainedMessageStore,
                sharedAsyncStoreExecutor,
                qos1InflightStore,
                qos2InflightStore,
                willMessageStore
        );
    }

    private static SharedAsyncStoreExecutor buildSharedAsyncStoreExecutor(StorageAsyncSettings settings) {
        if (settings == null || !settings.enabled()) {
            return null;
        }
        return new SharedAsyncStoreExecutor(
                "jmqx-store-async",
                settings.queueCapacity(),
                settings.workerCount(),
                settings.enqueueTimeoutMs()
        );
    }

    private static SecurityRuntime buildSecurityRuntime(StartupContext context) {
        AdminStateRepository adminStateRepository = buildAdminStateRepository(context.adminPanelSettings());
        EmbeddedAdminStateStore.SecurityConfig initialSecurityConfig = resolveInitialSecurityConfig(context, adminStateRepository);
        ReloadableAuthProvider authProvider = new ReloadableAuthProvider(AuthProviderFactory.create(context.authProperties()));
        ReloadableAclAuthorizer aclAuthorizer = new ReloadableAclAuthorizer(AclAuthorizerFactory.create(context.aclProperties()));
        applyRuntimeSecurityConfig(
                context.authProperties(),
                context.aclProperties(),
                authProvider,
                aclAuthorizer,
                initialSecurityConfig
        );
        AdminAuthRuntime adminAuthRuntime = new AdminAuthRuntime(
                resolveAdminAuthConfig(adminStateRepository, context.adminAuthSettings())
        );
        RuntimeClientBlacklist clientBlacklist = new RuntimeClientBlacklist();
        loadPersistedBlacklistEntries(
                adminStateRepository,
                context.adminSyncSettings().clusterId(),
                clientBlacklist
        );
        BuiltInDatabaseUserService builtInDatabaseUserService = new BuiltInDatabaseUserService();
        ClientAuthenticator clientAuthenticator = buildClientAuthenticator(adminAuthRuntime, authProvider);
        return new SecurityRuntime(
                adminStateRepository,
                initialSecurityConfig,
                authProvider,
                aclAuthorizer,
                adminAuthRuntime,
                clientBlacklist,
                clientAuthenticator,
                builtInDatabaseUserService
        );
    }

    private static EmbeddedAdminStateStore.SecurityConfig resolveInitialSecurityConfig(
            StartupContext context,
            AdminStateRepository adminStateRepository
    ) {
        String clusterId = context.adminSyncSettings().clusterId();
        if (adminStateRepository.hasSecurityConfig(clusterId)) {
            return adminStateRepository.getSecurityConfig(clusterId);
        }
        return buildInitialSecurityConfig(context.authProperties(), context.aclProperties());
    }

    private static ClientAuthenticator buildClientAuthenticator(
            AdminAuthRuntime adminAuthRuntime,
            ReloadableAuthProvider authProvider
    ) {
        return new ClientAuthenticator() {
            @Override
            public AuthResult authenticateResult(String clientId, String username, String password) {
                if (isAdminDashboardClient(adminAuthRuntime, clientId, username, password)) {
                    return AuthResult.allow(true);
                }
                return authProvider.authenticateResult(new AuthRequest(clientId, username, password));
            }

            @Override
            public CompletableFuture<AuthResult> authenticateAsync(String clientId, String username, String password) {
                if (isAdminDashboardClient(adminAuthRuntime, clientId, username, password)) {
                    return CompletableFuture.completedFuture(AuthResult.allow(true));
                }
                return authProvider.authenticateAsync(new AuthRequest(clientId, username, password));
            }

            @Override
            public void evictCache(String clientId, String username) {
                authProvider.evictCache(clientId, username);
            }
        };
    }

    private static BridgeRuntime buildBridgeRuntime(
            StartupContext context,
            AdminStateRepository adminStateRepository
    ) {
        ReloadableMessageBridge messageBridge = new ReloadableMessageBridge(
                MessageBridgeFactory.create(context.bridgeProperties())
        );
        EmbeddedAdminStateStore.BridgeConfig initialBridgeConfig = resolveInitialBridgeConfig(
                context,
                adminStateRepository
        );
        return new BridgeRuntime(
                messageBridge,
                initialBridgeConfig,
                new AtomicReference<>()
        );
    }

    private static EmbeddedAdminStateStore.BridgeConfig resolveInitialBridgeConfig(
            StartupContext context,
            AdminStateRepository adminStateRepository
    ) {
        String clusterId = context.adminSyncSettings().clusterId();
        if (adminStateRepository.hasBridgeConfig(clusterId)) {
            return adminStateRepository.getBridgeConfig(clusterId);
        }
        return buildInitialBridgeConfig(context.bridgeProperties());
    }

    private static MetadataRuntime startMetadataRuntime(
            StartupContext context,
            RetainedMessageStore retainedMessageStore,
            SecurityRuntime securityRuntime,
            BridgeRuntime bridgeRuntime
    ) {
        MetadataRuntime metadataRuntime = buildMetadataRuntime(
                context.clusterRoleProvider(),
                context.globalSubscriptionRegistry(),
                context.adminSyncSettings().clusterId(),
                context.sessionRegistry(),
                retainedMessageStore,
                securityRuntime.adminStateRepository(),
                securityRuntime.builtInDatabaseUserService(),
                securityRuntime.clientAuthenticator(),
                securityRuntime.clientBlacklist(),
                (clusterId, securityConfig) -> applyRuntimeSecurityConfig(
                        context.authProperties(),
                        context.aclProperties(),
                        securityRuntime.authProvider(),
                        securityRuntime.aclAuthorizer(),
                        securityConfig
                ),
                (clusterId, clusterConfig) -> {
                },
                (clusterId, bridgeConfig) -> applyRuntimeBridgeConfig(
                        context.bridgeProperties(),
                        bridgeRuntime.messageBridge(),
                        bridgeRuntime.brokerMessageHandlerRef().get(),
                        bridgeConfig
                ),
                context.clusterSettings().coreBindHost(),
                context.clusterSettings().coreBindPort(),
                context.clusterSettings().clusterRequestTimeoutMs(),
                context.clusterSettings().clusterReplayMaxEvents(),
                context.clusterSettings().raftGroupId(),
                context.clusterSettings().raftServerId(),
                context.clusterSettings().raftInitialConf(),
                context.clusterSettings().raftDataPath(),
                context.clusterSettings().raftElectionTimeoutMs(),
                context.clusterSettings().raftSnapshotIntervalSecs(),
                context.clusterSettings().clusterReconnectBackoffMs(),
                context.clusterSettings().clusterAckBatchSize(),
                context.clusterSettings().clusterAckFlushIntervalMs(),
                context.clusterSettings().clusterReplicantMaxInFlightEvents(),
                context.clusterSettings().clusterReplicantPushBatchSize(),
                context.clusterSettings().clusterNodeDownCleanupDelayMs()
        );
        metadataRuntime.replicator().start();
        return metadataRuntime;
    }

    private static ClusterDispatchRuntime buildClusterDispatchRuntime(StartupContext context) {
        NettyClusterMessageTransport transport = new NettyClusterMessageTransport(
                context.clusterRoleProvider().nodeId(),
                context.clusterSettings().clusterMessageBindHost(),
                context.clusterSettings().clusterMessageBindPort(),
                context.clusterSettings().clusterRequestTimeoutMs(),
                context.clusterSettings().clusterNodeEndpoints()
        );
        ClusterMessageDispatcher syncDispatcher = buildSyncClusterMessageDispatcher(transport);
        if (!context.clusterDispatchAsyncSettings().enabled()) {
            return new ClusterDispatchRuntime(transport, syncDispatcher, null);
        }
        AsyncClusterMessageDispatcher asyncDispatcher = new AsyncClusterMessageDispatcher(
                syncDispatcher,
                context.clusterDispatchAsyncSettings().queueCapacity(),
                context.clusterDispatchAsyncSettings().workerCount(),
                context.clusterDispatchAsyncSettings().enqueueTimeoutMs()
        );
        return new ClusterDispatchRuntime(transport, asyncDispatcher, asyncDispatcher);
    }

    private static ClusterMessageDispatcher buildSyncClusterMessageDispatcher(
            NettyClusterMessageTransport transport
    ) {
        return (topic, payload, publishQos, targetPlans) -> {
            if (targetPlans == null || targetPlans.isEmpty()) {
                return;
            }
            targetPlans.forEach((targetNodeId, target) -> transport.dispatch(
                    topic,
                    payload,
                    publishQos,
                    targetNodeId,
                    target.includeNormal(),
                    target.sharedGroups()
            ));
        };
    }

    private static MqttBrokerMessageHandler buildBrokerMessageHandler(
            StartupContext context,
            RuntimeStores stores,
            SharedSubscriptionManager sharedSubscriptionManager,
            SecurityRuntime securityRuntime,
            BridgeRuntime bridgeRuntime,
            MetadataRuntime metadataRuntime,
            ClusterMessageDispatcher clusterMessageDispatcher,
            ConnectionMetrics connectionMetrics,
            AdminReporter adminReporter
    ) {
        return new MqttBrokerMessageHandler(
                context.sessionRegistry(),
                context.subscriptionRegistry(),
                stores.retainedMessageStore(),
                securityRuntime.clientAuthenticator(),
                securityRuntime.clientBlacklist(),
                securityRuntime.aclAuthorizer(),
                sharedSubscriptionManager,
                bridgeRuntime.messageBridge(),
                bridgeRuntime.initialBridgeConfig().enabled(),
                context.retainedStoreProperties().isRetainedEnabled(),
                stores.qos1InflightStore(),
                stores.qos2InflightStore(),
                stores.willMessageStore(),
                context.brokerProperties().getMaxWillPayloadBytes(),
                context.globalSubscriptionRegistry(),
                context.clusterRoleProvider().nodeId(),
                metadataRuntime.gateway(),
                clusterMessageDispatcher,
                adminReporter,
                context.adminSyncSettings().clusterId(),
                context.bridgeProperties().getTopicFilters(),
                context.brokerProperties().getMaxQos(),
                context.brokerProperties().isPublishNoLocalEnabled(),
                context.brokerProperties().getMaxSubscriptionsPerClient(),
                BrokerRateLimitConfig.of(
                        context.brokerProperties().isRateLimitClientIdEnabled(),
                        context.brokerProperties().getRateLimitClientIdPerSecond(),
                        context.brokerProperties().isRateLimitIpEnabled(),
                        context.brokerProperties().getRateLimitIpPerSecond(),
                        context.brokerProperties().getRateLimitPublishStrategy(),
                        context.brokerProperties().isRateLimitConnectEnabled(),
                        context.brokerProperties().getRateLimitConnectGlobalPerSecond(),
                        context.brokerProperties().getRateLimitConnectIpPerSecond(),
                        context.brokerProperties().getRateLimitConnectStrategy(),
                        context.brokerProperties().getRateLimitCleanupIntervalSeconds(),
                        context.brokerProperties().getRateLimitIdleSeconds()
                )
        );
    }

    private static AdminPanelServer buildAdminPanelServer(
            StartupContext context,
            SecurityRuntime securityRuntime,
            BridgeRuntime bridgeRuntime,
            MetadataRuntime metadataRuntime,
            ConnectionMetrics connectionMetrics,
            MqttBrokerMessageHandler brokerMessageHandler
    ) {
        return startAdminPanelServer(
                context.adminPanelSettings(),
                context.adminSyncSettings().clusterId(),
                context.clusterRoleProvider().nodeId(),
                context.clusterRoleProvider().role().name(),
                context.sessionRegistry(),
                context.subscriptionRegistry(),
                connectionMetrics,
                brokerMessageHandler,
                securityRuntime.adminAuthRuntime(),
                securityRuntime.adminStateRepository(),
                securityRuntime.builtInDatabaseUserService(),
                securityRuntime.initialSecurityConfig(),
                buildInitialClusterConfig(context),
                bridgeRuntime.initialBridgeConfig(),
                buildSecurityConfigUpdater(context, metadataRuntime),
                buildClusterConfigUpdater(context, metadataRuntime),
                buildBridgeConfigUpdater(context, metadataRuntime),
                buildClientKickUpdater(context, metadataRuntime),
                buildBlacklistUpdater(context, metadataRuntime),
                buildBuiltInDatabaseUsersUpdater(context, metadataRuntime, securityRuntime.builtInDatabaseUserService())
        );
    }

    private static AdminPanelServer.SecurityConfigUpdater buildSecurityConfigUpdater(
            StartupContext context,
            MetadataRuntime metadataRuntime
    ) {
        return (clusterId, securityConfig) -> metadataRuntime.gateway().submit(new MetadataCommand(
                ClusterMetadataCommandApplier.ADMIN_SECURITY_NAMESPACE,
                "upsert",
                clusterId,
                AdminConfigCodec.encodeSecurityConfigToString(securityConfig),
                context.clusterRoleProvider().nodeId()
        ));
    }

    private static AdminPanelServer.ClusterConfigUpdater buildClusterConfigUpdater(
            StartupContext context,
            MetadataRuntime metadataRuntime
    ) {
        return (clusterId, clusterConfig) -> metadataRuntime.gateway().submit(new MetadataCommand(
                ClusterMetadataCommandApplier.ADMIN_CLUSTER_NAMESPACE,
                "upsert",
                clusterId,
                AdminConfigCodec.encodeClusterConfigToString(clusterConfig),
                context.clusterRoleProvider().nodeId()
        ));
    }

    private static AdminPanelServer.BridgeConfigUpdater buildBridgeConfigUpdater(
            StartupContext context,
            MetadataRuntime metadataRuntime
    ) {
        return (clusterId, bridgeConfig) -> metadataRuntime.gateway().submit(new MetadataCommand(
                ClusterMetadataCommandApplier.ADMIN_BRIDGE_NAMESPACE,
                "upsert",
                clusterId,
                AdminConfigCodec.encodeBridgeConfigToString(bridgeConfig),
                context.clusterRoleProvider().nodeId()
        ));
    }

    private static AdminPanelServer.ClientKickUpdater buildClientKickUpdater(
            StartupContext context,
            MetadataRuntime metadataRuntime
    ) {
        return (clusterId, clientId) -> metadataRuntime.gateway().submit(new MetadataCommand(
                ClusterMetadataCommandApplier.SESSION_NAMESPACE,
                "kick",
                clientId,
                "admin",
                context.clusterRoleProvider().nodeId()
        ));
    }

    private static AdminPanelServer.BlacklistUpdater buildBlacklistUpdater(
            StartupContext context,
            MetadataRuntime metadataRuntime
    ) {
        return new AdminPanelServer.BlacklistUpdater() {
            @Override
            public void upsert(String clusterId, EmbeddedAdminStateStore.BlacklistEntry entry) {
                metadataRuntime.gateway().submit(new MetadataCommand(
                        ClusterMetadataCommandApplier.ADMIN_BLACKLIST_NAMESPACE,
                        "upsert",
                        clusterId,
                        AdminConfigCodec.encodeBlacklistEntryToString(entry),
                        context.clusterRoleProvider().nodeId()
                ));
            }

            @Override
            public void delete(String clusterId, String type, String value) {
                metadataRuntime.gateway().submit(new MetadataCommand(
                        ClusterMetadataCommandApplier.ADMIN_BLACKLIST_NAMESPACE,
                        "delete",
                        clusterId,
                        AdminConfigCodec.encodeBlacklistEntryToString(
                                new EmbeddedAdminStateStore.BlacklistEntry(
                                        type,
                                        value,
                                        System.currentTimeMillis(),
                                        context.clusterRoleProvider().nodeId()
                                )
                        ),
                        context.clusterRoleProvider().nodeId()
                ));
            }
        };
    }

    private static AdminPanelServer.BuiltInDatabaseUsersUpdater buildBuiltInDatabaseUsersUpdater(
            StartupContext context,
            MetadataRuntime metadataRuntime,
            BuiltInDatabaseUserService builtInDatabaseUserService
    ) {
        return new AdminPanelServer.BuiltInDatabaseUsersUpdater() {
            @Override
            public void upsert(
                    String clusterId,
                    EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config,
                    String userId,
                    String password,
                    boolean superuser
            ) {
                String encodedCredential = builtInDatabaseUserService.encodeUserCredential(config, password, superuser);
                metadataRuntime.gateway().submit(new MetadataCommand(
                        ClusterMetadataCommandApplier.ADMIN_BUILT_IN_USER_NAMESPACE,
                        "upsert",
                        userId,
                        encodedCredential,
                        context.clusterRoleProvider().nodeId()
                ));
            }

            @Override
            public int importUsers(
                    String clusterId,
                    EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config,
                    List<BuiltInDatabaseUserService.UserInput> users
            ) {
                int count = 0;
                if (users == null) {
                    return 0;
                }
                for (BuiltInDatabaseUserService.UserInput user : users) {
                    if (user == null || user.userId() == null || user.userId().isBlank()
                            || user.password() == null || user.password().isBlank()) {
                        continue;
                    }
                    upsert(clusterId, config, user.userId(), user.password(), user.superuser());
                    count++;
                }
                return count;
            }

            @Override
            public void delete(String clusterId, String userId) {
                metadataRuntime.gateway().submit(new MetadataCommand(
                        ClusterMetadataCommandApplier.ADMIN_BUILT_IN_USER_NAMESPACE,
                        "delete",
                        userId,
                        "",
                        context.clusterRoleProvider().nodeId()
                ));
            }

            @Override
            public void deleteAll(String clusterId) {
                metadataRuntime.gateway().submit(new MetadataCommand(
                        ClusterMetadataCommandApplier.ADMIN_BUILT_IN_USER_NAMESPACE,
                        "clear",
                        "",
                        "",
                        context.clusterRoleProvider().nodeId()
                ));
            }
        };
    }

    /**
     * Starts the broker network endpoints.
     */
    private static EndpointServers startEndpointServers(
            BrokerProperties brokerProperties,
            MqttBrokerMessageHandler brokerMessageHandler,
            ConnectionMetrics connectionMetrics) throws InterruptedException {

        // Start the plain MQTT endpoint.
        NettyMqttEndpointServer mqttServer = new NettyMqttEndpointServer(
                brokerProperties,
                brokerMessageHandler,
                connectionMetrics,
                new NettyMqttEndpointServer.EndpointSpec(
                        true,
                        brokerProperties.getHost(),
                        brokerProperties.getPort(),
                        false,
                        false,
                        null,
                        "mqtt",
                        "MQTT"
                )
        );
        mqttServer.start();

        // Start the TLS MQTT endpoint.
        NettyMqttEndpointServer mqttTlsServer = new NettyMqttEndpointServer(
                brokerProperties,
                brokerMessageHandler,
                connectionMetrics,
                new NettyMqttEndpointServer.EndpointSpec(
                        brokerProperties.isMqttsEnabled(),
                        brokerProperties.getMqttsHost(),
                        brokerProperties.getMqttsPort(),
                        true,
                        false,
                        null,
                        "mqtts",
                        "MQTTS"
                )
        );
        mqttTlsServer.start();

        // Start the WebSocket endpoint.
        NettyMqttEndpointServer mqttWebSocketServer = new NettyMqttEndpointServer(
                brokerProperties,
                brokerMessageHandler,
                connectionMetrics,
                new NettyMqttEndpointServer.EndpointSpec(
                        brokerProperties.isWebsocketEnabled(),
                        brokerProperties.getWebsocketHost(),
                        brokerProperties.getWebsocketPort(),
                        false,
                        true,
                        brokerProperties.getWebsocketPath(),
                        "websocket",
                        "WS"
                )
        );
        mqttWebSocketServer.start();

        // Start the secure WebSocket endpoint.
        NettyMqttEndpointServer mqttWssServer = new NettyMqttEndpointServer(
                brokerProperties,
                brokerMessageHandler,
                connectionMetrics,
                new NettyMqttEndpointServer.EndpointSpec(
                        brokerProperties.isWssEnabled(),
                        brokerProperties.getWssHost(),
                        brokerProperties.getWssPort(),
                        true,
                        true,
                        brokerProperties.getWssPath(),
                        "wss",
                        "WSS"
                )
        );
        mqttWssServer.start();
        return new EndpointServers(mqttServer, mqttTlsServer, mqttWebSocketServer, mqttWssServer);
    }

    /**
     * Registers graceful shutdown in reverse dependency order.
     */
    private static void registerShutdownHook(RuntimeComponents runtimeComponents) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            runtimeComponents.adminPanelServer().stop();
            runtimeComponents.dashboardPublisher().shutdownNow();
            runtimeComponents.brokerMessageHandler().shutdown();
            runtimeComponents.authProvider().close();
            runtimeComponents.retainedMessageStore().close();
            runtimeComponents.willMessageStore().close();
            closeQuietly(runtimeComponents.storageAsyncExecutorCloser());
            runtimeComponents.messageBridge().close();
            closeQuietly(runtimeComponents.clusterMessageDispatcherCloser());
            runtimeComponents.clusterMessageTransport().stop();
            runtimeComponents.metadataRuntime().replicator().stop();
            runtimeComponents.endpointServers().mqttWssServer().stop();
            runtimeComponents.endpointServers().mqttWebSocketServer().stop();
            runtimeComponents.endpointServers().mqttTlsServer().stop();
            runtimeComponents.endpointServers().mqttServer().stop();
        }));
    }

    private static void printStartupSummary(StartupContext context) {
        BrokerProperties brokerProperties = context.brokerProperties();
        ClusterRoleProvider clusterRoleProvider = context.clusterRoleProvider();
        ClusterSettings clusterSettings = context.clusterSettings();
        LOG.info("JMQX started on {}:{}", brokerProperties.getHost(), brokerProperties.getPort());
        if (brokerProperties.isMqttsEnabled()) {
            String mqttsHost = toDisplayHost(brokerProperties.getMqttsHost());
            LOG.info("JMQX mqtts: mqtts://{}:{}", mqttsHost, brokerProperties.getMqttsPort());
        }
        if (brokerProperties.isWebsocketEnabled()) {
            String wsHost = toDisplayHost(brokerProperties.getWebsocketHost());
            String wsPath = normalizeWebsocketPath(brokerProperties.getWebsocketPath());
            LOG.info("JMQX websocket: ws://{}:{}{}", wsHost, brokerProperties.getWebsocketPort(), wsPath);
        }
        if (brokerProperties.isWssEnabled()) {
            String wssHost = toDisplayHost(brokerProperties.getWssHost());
            String wssPath = normalizeWebsocketPath(brokerProperties.getWssPath());
            LOG.info("JMQX wss: wss://{}:{}{}", wssHost, brokerProperties.getWssPort(), wssPath);
        }
        LOG.info("AUTH chain: {}", context.authProperties().getChain());
        LOG.info("ACL chain: {}", context.aclProperties().getChain());
        LOG.info("BRIDGE enabled={}, types={}, topicFilters={}",
                context.bridgeProperties().isEnabled(),
                context.bridgeProperties().getTypes(),
                context.bridgeProperties().getTopicFilters());
        LOG.info("RATE_LIMIT clientIdEnabled={}, clientIdPerSecond={}, ipEnabled={}, ipPerSecond={}, connectEnabled={}, connectGlobalPerSecond={}, connectIpPerSecond={}, cleanupIntervalSeconds={}, idleSeconds={}",
                brokerProperties.isRateLimitClientIdEnabled(),
                brokerProperties.getRateLimitClientIdPerSecond(),
                brokerProperties.isRateLimitIpEnabled(),
                brokerProperties.getRateLimitIpPerSecond(),
                brokerProperties.isRateLimitConnectEnabled(),
                brokerProperties.getRateLimitConnectGlobalPerSecond(),
                brokerProperties.getRateLimitConnectIpPerSecond(),
                brokerProperties.getRateLimitCleanupIntervalSeconds(),
                brokerProperties.getRateLimitIdleSeconds());
        LOG.info("BROKER maxWillPayloadBytes={}, maxSubscriptionsPerClient={}",
                brokerProperties.getMaxWillPayloadBytes(),
                brokerProperties.getMaxSubscriptionsPerClient());
        LOG.info("RETAINED maxEntries={}, maxBytes={}, maxPayloadBytes={}, rocksdbPath={}, enabled={}, overflowStrategy={}",
                context.retainedStoreProperties().getMaxEntries(),
                context.retainedStoreProperties().getMaxBytes(),
                context.retainedStoreProperties().getMaxPayloadBytes(),
                context.retainedStoreProperties().getRocksdbPath(),
                context.retainedStoreProperties().isRetainedEnabled(),
                context.retainedStoreProperties().getOverflowStrategy());
        LOG.info("QOS1 inflight rocksdbPath={}", context.qos1InflightRocksdbPath());
        LOG.info("QOS2 inflight rocksdbPath={}", context.qos2InflightRocksdbPath());
        LOG.info("WILL persist enabled={}, rocksdbPath={}",
                context.willPersistEnabled(),
                context.willRocksdbPath());
        LOG.info("SHARED maxSubscribersPerGroup={}, slowConsumerStrikeThreshold={}",
                context.sharedMaxSubscribers(),
                context.sharedSlowThreshold());
        LOG.info("CLUSTER-DISPATCH asyncEnabled={}, queueCapacity={}, workerCount={}, enqueueTimeoutMs={}",
                context.clusterDispatchAsyncSettings().enabled(),
                context.clusterDispatchAsyncSettings().queueCapacity(),
                context.clusterDispatchAsyncSettings().workerCount(),
                context.clusterDispatchAsyncSettings().enqueueTimeoutMs());
        LOG.info("STORE-ASYNC enabled={}, queueCapacity={}, workerCount={}, enqueueTimeoutMs={}",
                context.storageAsyncSettings().enabled(),
                context.storageAsyncSettings().queueCapacity(),
                context.storageAsyncSettings().workerCount(),
                context.storageAsyncSettings().enqueueTimeoutMs());
        LOG.info("ADMIN-SYNC enabled={}, url={}, clusterId={}, nodeIp={}, metricsIntervalMs={}, dashboardPublishIntervalMs={}",
                context.adminSyncSettings().enabled(),
                context.adminSyncSettings().url(),
                context.adminSyncSettings().clusterId(),
                context.adminSyncSettings().nodeIp(),
                context.adminSyncSettings().metricsIntervalMs(),
                context.adminSyncSettings().dashboardPublishIntervalMs());
        LOG.info("ADMIN-PANEL enabled={}, host={}, port={}, path={}, backendUrl={}",
                context.adminPanelSettings().enabled(),
                context.adminPanelSettings().host(),
                context.adminPanelSettings().port(),
                context.adminPanelSettings().basePath(),
                context.adminPanelSettings().backendUrl());
        LOG.info("CLUSTER role={}, nodeId={}, transport=netty, coreEndpoints={}, coreBindHost={}, coreBindPort={}, nettyRequestTimeoutMs={}, reconnectBackoffMs={}, ackBatchSize={}, ackFlushIntervalMs={}, replicantMaxInFlightEvents={}, replicantPushBatchSize={}, nodeDownCleanupDelayMs={}, messageBindHost={}, messageBindPort={}, nodeEndpoints={}, replayMaxEvents={}, raftGroupId={}, raftServerId={}, raftInitialConf={}, raftDataPath={}",
                clusterRoleProvider.role(),
                clusterRoleProvider.nodeId(),
                clusterRoleProvider.coreEndpoints(),
                clusterSettings.coreBindHost(),
                clusterSettings.coreBindPort(),
                clusterSettings.clusterRequestTimeoutMs(),
                clusterSettings.clusterReconnectBackoffMs(),
                clusterSettings.clusterAckBatchSize(),
                clusterSettings.clusterAckFlushIntervalMs(),
                clusterSettings.clusterReplicantMaxInFlightEvents(),
                clusterSettings.clusterReplicantPushBatchSize(),
                clusterSettings.clusterNodeDownCleanupDelayMs(),
                clusterSettings.clusterMessageBindHost(),
                clusterSettings.clusterMessageBindPort(),
                clusterSettings.clusterNodeEndpoints(),
                clusterSettings.clusterReplayMaxEvents(),
                clusterSettings.raftGroupId(),
                clusterSettings.raftServerId(),
                clusterSettings.raftInitialConf(),
                clusterSettings.raftDataPath());
    }

    private static RetainedMessageStore buildRetainedMessageStore(RetainedStoreProperties properties) {
        return new RocksDbRetainedMessageStore(properties);
    }

    private static Qos1InflightStore buildQos1InflightStore(
            StartupContext context,
            SharedAsyncStoreExecutor sharedAsyncStoreExecutor
    ) {
        Qos1InflightStore store = new RocksDbQos1InflightStore(context.qos1InflightRocksdbPath());
        if (!context.storageAsyncSettings().enabled() || sharedAsyncStoreExecutor == null) {
            return store;
        }
        return new AsyncQos1InflightStore(store, sharedAsyncStoreExecutor);
    }

    private static Qos2InflightStore buildQos2InflightStore(
            StartupContext context,
            SharedAsyncStoreExecutor sharedAsyncStoreExecutor
    ) {
        // QoS2 prefers synchronous persistence to keep the post-ACK crash window small.
        return new RocksDbQos2InflightStore(context.qos2InflightRocksdbPath());
    }

    private static WillMessageStore buildWillMessageStore(
            StartupContext context,
            SharedAsyncStoreExecutor sharedAsyncStoreExecutor
    ) {
        if (!context.willPersistEnabled()) {
            return WillMessageStore.NOOP;
        }
        WillMessageStore store = new RocksDbWillMessageStore(context.willRocksdbPath());
        if (!context.storageAsyncSettings().enabled() || sharedAsyncStoreExecutor == null) {
            return store;
        }
        return new AsyncWillMessageStore(store, sharedAsyncStoreExecutor);
    }

    private static AdminSyncSettings loadAdminSyncSettings(JmqxConfig config) {
        return new AdminSyncSettings(
                getBooleanProperty(config, "jmqx.admin.enabled", false),
                getStringProperty(config, "jmqx.admin.url", "http://127.0.0.1:18081"),
                getStringProperty(config, "jmqx.admin.clusterId", "default"),
                getStringProperty(config, "jmqx.admin.nodeIp", ""),
                getIntProperty(config, "jmqx.admin.http.connectTimeoutMs", 1500),
                getIntProperty(config, "jmqx.admin.http.requestTimeoutMs", 3000),
                getIntProperty(config, "jmqx.admin.metrics.intervalMs", 5000),
                getIntProperty(config, "jmqx.admin.dashboard.publishIntervalMs", 2000)
        );
    }

    private static AdminAuthSettings loadAdminAuthSettings(JmqxConfig config) {
        return new AdminAuthSettings(
                getStringProperty(config, "jmqx.admin.auth.username", "admin"),
                getStringProperty(config, "jmqx.admin.auth.password", "public"),
                getStringProperty(config, "jmqx.admin.auth.role", "super_admin")
        );
    }

    private static AdminPanelSettings loadAdminPanelSettings(JmqxConfig config) {
        return new AdminPanelSettings(
                getBooleanProperty(config, "jmqx.admin.panel.enabled", true),
                getStringProperty(config, "jmqx.admin.panel.host", "0.0.0.0"),
                getIntProperty(config, "jmqx.admin.panel.port", 18081),
                getStringProperty(config, "jmqx.admin.panel.basePath", "/admin"),
                getStringProperty(config, "jmqx.admin.panel.backendUrl", "http://127.0.0.1:18080"),
                getBooleanProperty(config, "jmqx.admin.panel.persistence.enabled", true),
                getStringProperty(config, "jmqx.admin.panel.persistence.rocksdb.path", "data/admin-state-rocksdb")
        );
    }

    private static AdminReporter buildAdminReporter(
            AdminSyncSettings settings,
            AdminAuthRuntime adminAuthRuntime,
            String nodeId,
            SessionRegistry sessionRegistry,
            ConnectionMetrics connectionMetrics
    ) {
        if (settings == null || !settings.enabled()) {
            return AdminReporter.NOOP;
        }
        String nodeIp = settings.nodeIp();
        if (nodeIp == null || nodeIp.isBlank()) {
            nodeIp = resolveLocalNodeIp();
        }
        return new HttpAdminReporter(
                settings.url(),
                settings.clusterId(),
                adminAuthRuntime == null ? new AdminAuthRuntime(AdminAuthRuntime.Config.defaults()) : adminAuthRuntime,
                nodeId,
                nodeIp,
                sessionRegistry,
                connectionMetrics,
                settings.connectTimeoutMs(),
                settings.requestTimeoutMs(),
                settings.metricsIntervalMs()
        );
    }

    /**
     * Starts the periodic dashboard publisher used by the embedded admin console.
     */
    private static ScheduledExecutorService startDashboardPublisher(
            MqttBrokerMessageHandler brokerMessageHandler,
            String nodeId,
            String nodeRole,
            String clusterId,
            String configuredNodeIp,
            SessionRegistry sessionRegistry,
            ConnectionMetrics connectionMetrics,
            long publishIntervalMs
    ) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "jmqx-dashboard-publisher");
            thread.setDaemon(true);
            return thread;
        });
        String safeClusterId = (clusterId == null || clusterId.isBlank()) ? "default" : clusterId.trim();
        String nodeIp = configuredNodeIp;
        if (nodeIp == null || nodeIp.isBlank()) {
            nodeIp = resolveLocalNodeIp();
        }
        final String topic = "$SYS/dashboard/" + safeClusterId + "/cluster/overview";
        final String safeNodeIp = nodeIp;
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                int connectedClients = sessionRegistry == null ? 0 : sessionRegistry.list().size();
                SecurityPipelineMetrics.Snapshot securitySnapshot =
                        brokerMessageHandler == null ? null : brokerMessageHandler.securityPipelineMetricsSnapshot();
                String payload = "{"
                        + "\"clusterId\":\"" + safeJson(safeClusterId) + "\","
                        + "\"nodeId\":\"" + safeJson(nodeId) + "\","
                        + "\"role\":\"" + safeJson(nodeRole) + "\","
                        + "\"nodeIp\":\"" + safeJson(safeNodeIp) + "\","
                        + "\"connections\":" + connectedClients + ","
                        + "\"inboundBytes\":" + connectionMetrics.getInboundBytes() + ","
                        + "\"outboundBytes\":" + connectionMetrics.getOutboundBytes() + ","
                        + "\"connectAuthSuccess\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::connectAuthSuccess) + ","
                        + "\"connectAuthFailure\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::connectAuthFailure) + ","
                        + "\"connectAuthError\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::connectAuthError) + ","
                        + "\"connectAuthSlow\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::connectAuthSlow) + ","
                        + "\"connectAuthAvgMs\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::connectAuthAvgMs) + ","
                        + "\"connectAuthMaxMs\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::connectAuthMaxMs) + ","
                        + "\"publishAclAllow\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::publishAclAllow) + ","
                        + "\"publishAclDeny\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::publishAclDeny) + ","
                        + "\"publishAclError\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::publishAclError) + ","
                        + "\"publishAclSlow\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::publishAclSlow) + ","
                        + "\"publishAclAvgMs\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::publishAclAvgMs) + ","
                        + "\"publishAclMaxMs\":" + metricValue(securitySnapshot, SecurityPipelineMetrics.Snapshot::publishAclMaxMs) + ","
                        + "\"timestamp\":" + System.currentTimeMillis()
                        + "}";
                assert brokerMessageHandler != null;
                brokerMessageHandler.publishSystemTopic(topic, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } catch (Exception ignored) {
            }
        }, 1500, Math.max(500, publishIntervalMs), TimeUnit.MILLISECONDS);
        return scheduler;
    }

    private static long metricValue(
            SecurityPipelineMetrics.Snapshot snapshot,
            java.util.function.ToLongFunction<SecurityPipelineMetrics.Snapshot> extractor
    ) {
        if (snapshot == null || extractor == null) {
            return 0L;
        }
        return extractor.applyAsLong(snapshot);
    }

    private static AdminPanelServer startAdminPanelServer(
            AdminPanelSettings settings,
            String clusterId,
            String nodeId,
            String nodeRole,
            SessionRegistry sessionRegistry,
            SubscriptionRegistry subscriptionRegistry,
            ConnectionMetrics connectionMetrics,
            MqttBrokerMessageHandler brokerMessageHandler,
            AdminAuthRuntime adminAuthRuntime,
            AdminStateRepository stateRepository,
            BuiltInDatabaseUserService builtInDatabaseUserService,
            EmbeddedAdminStateStore.SecurityConfig initialSecurityConfig,
            EmbeddedAdminStateStore.ClusterConfig initialClusterConfig,
            EmbeddedAdminStateStore.BridgeConfig initialBridgeConfig,
            AdminPanelServer.SecurityConfigUpdater securityConfigUpdater,
            AdminPanelServer.ClusterConfigUpdater clusterConfigUpdater,
            AdminPanelServer.BridgeConfigUpdater bridgeConfigUpdater,
            AdminPanelServer.ClientKickUpdater clientKickUpdater,
            AdminPanelServer.BlacklistUpdater blacklistUpdater,
            AdminPanelServer.BuiltInDatabaseUsersUpdater builtInDatabaseUsersUpdater
    ) {
        String panelNodeIp = resolveLocalNodeIp();
        AdminPanelServer server = new AdminPanelServer(
                settings.host(),
                settings.port(),
                settings.basePath(),
                settings.backendUrl(),
                adminAuthRuntime,
                clusterId,
                nodeId,
                panelNodeIp,
                nodeRole,
                sessionRegistry,
                subscriptionRegistry,
                connectionMetrics,
                () -> brokerMessageHandler == null ? null : brokerMessageHandler.securityPipelineMetricsSnapshot(),
                stateRepository,
                initialSecurityConfig,
                initialClusterConfig,
                initialBridgeConfig,
                securityConfigUpdater,
                clusterConfigUpdater,
                bridgeConfigUpdater,
                clientKickUpdater,
                blacklistUpdater,
                builtInDatabaseUsersUpdater,
                builtInDatabaseUserService
        );
        if (settings.enabled()) {
            server.start();
        }
        return server;
    }

    private static AdminStateRepository buildAdminStateRepository(AdminPanelSettings settings) {
        if (settings == null || !settings.persistenceEnabled()) {
            return new EmbeddedAdminStateStore();
        }
        return new RocksDbAdminStateStore(settings.persistenceRocksdbPath());
    }

    private static AdminAuthRuntime.Config resolveAdminAuthConfig(
            AdminStateRepository adminStateRepository,
            AdminAuthSettings adminAuthSettings
    ) {
        if (adminStateRepository != null && adminStateRepository.hasAdminAuthConfig()) {
            AdminAuthRuntime.Config persisted = adminStateRepository.getAdminAuthConfig();
            if (persisted != null) {
                return persisted.normalize();
            }
        }
        AdminAuthRuntime.Config fallback = new AdminAuthRuntime.Config(
                adminAuthSettings == null ? "admin" : adminAuthSettings.username(),
                adminAuthSettings == null ? "public" : adminAuthSettings.password(),
                adminAuthSettings == null ? "super_admin" : adminAuthSettings.role()
        ).normalize();
        if (adminStateRepository != null && !adminStateRepository.hasAdminAuthConfig()) {
            adminStateRepository.setAdminAuthConfig(fallback);
        }
        return fallback;
    }

    private static void loadPersistedBlacklistEntries(
            AdminStateRepository adminStateRepository,
            String clusterId,
            ClientBlacklist clientBlacklist
    ) {
        if (adminStateRepository == null || clientBlacklist == null) {
            return;
        }
        String effectiveClusterId = (clusterId == null || clusterId.isBlank()) ? "default" : clusterId.trim();
        for (EmbeddedAdminStateStore.BlacklistEntry entry : adminStateRepository.listBlacklistEntries(effectiveClusterId)) {
            if (entry == null) {
                continue;
            }
            clientBlacklist.upsert(entry.type(), entry.value());
        }
    }

    private static MetadataRuntime buildMetadataRuntime(
            ClusterRoleProvider clusterRoleProvider,
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            String localClusterId,
            SessionRegistry sessionRegistry,
            RetainedMessageStore retainedMessageStore,
            AdminStateRepository adminStateRepository,
            BuiltInDatabaseUserService builtInDatabaseUserService,
            ClientAuthenticator clientAuthenticator,
            ClientBlacklist clientBlacklist,
            JmqxMetadataProjectionHandlers.AdminSecurityConfigApplier adminSecurityConfigApplier,
            JmqxMetadataProjectionHandlers.AdminClusterConfigApplier adminClusterConfigApplier,
            JmqxMetadataProjectionHandlers.AdminBridgeConfigApplier adminBridgeConfigApplier,
            String coreBindHost,
            int coreBindPort,
            int clusterRequestTimeoutMs,
            int clusterReplayMaxEvents,
            String raftGroupId,
            String raftServerId,
            String raftInitialConf,
            String raftDataPath,
            int raftElectionTimeoutMs,
            int raftSnapshotIntervalSecs,
            int clusterReconnectBackoffMs,
            int clusterAckBatchSize,
            int clusterAckFlushIntervalMs,
            int clusterReplicantMaxInFlightEvents,
            int clusterReplicantPushBatchSize,
            int clusterNodeDownCleanupDelayMs
    ) {
        JmqxMetadataProjectionHandlers handlers = new JmqxMetadataProjectionHandlers(
                globalSubscriptionRegistry,
                localClusterId,
                sessionRegistry,
                retainedMessageStore,
                adminStateRepository,
                builtInDatabaseUserService,
                clientAuthenticator,
                clientBlacklist,
                adminSecurityConfigApplier,
                adminClusterConfigApplier,
                adminBridgeConfigApplier
        );
        ClusterMetadataCommandApplier commandApplier = new ClusterMetadataCommandApplier(
                clusterRoleProvider.nodeId(),
                handlers::applyRouteSubscriptionCommand,
                handlers::applyClientOnlineCommand,
                handlers::applyRetainedCommand,
                handlers::applyAdminSecurityConfigCommand,
                handlers::applyAdminClusterConfigCommand,
                handlers::applyAdminBridgeConfigCommand,
                handlers::applyBuiltInUserCommand,
                handlers::applyAdminBlacklistCommand
        );
        if (clusterRoleProvider.role() == NodeRole.CORE) {
            SofaJraftMetadataCommandGateway raftGateway = new SofaJraftMetadataCommandGateway(
                    raftGroupId,
                    raftServerId,
                    raftInitialConf,
                    raftDataPath,
                    raftElectionTimeoutMs,
                    raftSnapshotIntervalSecs,
                    clusterRequestTimeoutMs
            );
            raftGateway.registerApplier(commandApplier::apply);
            NettyMetadataCoreServer coreServer = new NettyMetadataCoreServer(
                    coreBindHost,
                    coreBindPort,
                    raftGateway,
                    () -> buildRouteSnapshot(globalSubscriptionRegistry),
                    clusterReplayMaxEvents,
                    clusterReplicantMaxInFlightEvents,
                    clusterReplicantPushBatchSize,
                    clusterNodeDownCleanupDelayMs
            );
            raftGateway.registerApplier(coreServer);
            MetadataCommandGateway coreWriteGateway = buildCoreWriteGateway(
                    raftGateway,
                    clusterRoleProvider.coreEndpoints(),
                    clusterRequestTimeoutMs
            );
            return new MetadataRuntime(coreWriteGateway, new CompositeMetadataReplicator(raftGateway, coreServer));
        }
        MetadataCommandGateway gateway = new NettyMetadataCommandGateway(
                clusterRoleProvider.coreEndpoints(),
                clusterRequestTimeoutMs
        );
        MetadataReplicator replicator = new NettyMetadataReplicantSyncClient(
                clusterRoleProvider.nodeId(),
                clusterRoleProvider.coreEndpoints(),
                clusterReconnectBackoffMs,
                clusterAckBatchSize,
                clusterAckFlushIntervalMs,
                commandApplier::apply,
                globalSubscriptionRegistry::clear
        );
        return new MetadataRuntime(gateway, replicator);
    }

    private static MetadataCommandGateway buildCoreWriteGateway(
            SofaJraftMetadataCommandGateway localGateway,
            Set<String> coreEndpoints,
            int clusterRequestTimeoutMs
    ) {
        if (localGateway == null) {
            throw new IllegalArgumentException("localGateway is null");
        }
        MetadataCommandGateway remoteGateway = new NettyMetadataCommandGateway(coreEndpoints, clusterRequestTimeoutMs);
        return command -> {
            long localCommitted = localGateway.submit(command);
            if (localCommitted >= 0) {
                return localCommitted;
            }
            return remoteGateway.submit(command);
        };
    }

    private static MetadataSnapshot buildRouteSnapshot(GlobalSubscriptionRegistry globalSubscriptionRegistry) {
        Map<String, Set<String>> nodeToTopicKeys = globalSubscriptionRegistry.snapshotNodeToTopicKeys();
        List<MetadataCommand> commands = new ArrayList<>();
        nodeToTopicKeys.forEach((nodeId, topicKeys) -> {
            if (nodeId == null || nodeId.isBlank() || topicKeys == null || topicKeys.isEmpty()) {
                return;
            }
            for (String topicKey : topicKeys) {
                ParsedTopicKey parsed = ParsedTopicKey.parse(topicKey);
                if (parsed == null) {
                    continue;
                }
                commands.add(new MetadataCommand(
                        "route.subscription",
                        "register",
                        parsed.topicFilter(),
                        parsed.sharedGroup(),
                        nodeId
                ));
            }
        });
        return new MetadataSnapshot(globalSubscriptionRegistry.appliedLogIndex(), commands);
    }

    private static EmbeddedAdminStateStore.SecurityConfig buildInitialSecurityConfig(
            AuthProperties authProperties,
            AclProperties aclProperties
    ) {
        List<String> authChain = JmqxConfigMappers.resolveAuthChain(authProperties);
        List<String> aclChain = JmqxConfigMappers.resolveAclChain(aclProperties);
        boolean authEnabled = !authChain.isEmpty() && !"allow_all".equalsIgnoreCase(JmqxConfigMappers.firstOrEmpty(authChain));
        boolean aclEnabled = !aclChain.isEmpty();
        long cacheTtlMs = Math.max(authProperties.getCacheMillis(), aclProperties.getCacheMillis());
        return new EmbeddedAdminStateStore.SecurityConfig(
                aclEnabled,
                aclChain,
                aclProperties.isDefaultAllow(),
                new EmbeddedAdminStateStore.AclHttpConfig(
                        aclProperties.getHttpUrl(),
                        aclProperties.getHttpTimeoutMs(),
                        aclProperties.getHttpBodyTemplate()
                ),
                new EmbeddedAdminStateStore.AclFileConfig(
                        aclProperties.getFilePath()
                ),
                new EmbeddedAdminStateStore.AclRedisConfig(
                        aclProperties.getRedisHost(),
                        aclProperties.getRedisPort(),
                        aclProperties.getRedisPassword(),
                        aclProperties.getRedisDb(),
                        aclProperties.getRedisKeyPrefix(),
                        aclProperties.getRedisTimeoutMs()
                ),
                authEnabled,
                authChain,
                Math.max(cacheTtlMs, 0),
                new EmbeddedAdminStateStore.AuthHttpConfig(
                        authProperties.getHttpMethod(),
                        authProperties.getHttpUrl(),
                        authProperties.getHttpHeaders(),
                        authProperties.isHttpTlsEnabled(),
                        authProperties.getHttpBodyTemplate(),
                        authProperties.getHttpPoolSize(),
                        authProperties.getHttpRateLimitPerSecond(),
                        authProperties.getHttpRequestTimeoutMs(),
                        authProperties.getHttpConnectTimeoutMs(),
                        authProperties.getHttpPipelineCount()
                ),
                new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                        authProperties.getBuiltInDatabaseAccountType(),
                        authProperties.getBuiltInDatabasePasswordHashAlgorithm(),
                        authProperties.getBuiltInDatabaseSaltPosition()
                ),
                new EmbeddedAdminStateStore.AuthRedisConfig(
                        authProperties.getRedisHost(),
                        authProperties.getRedisPort(),
                        authProperties.getRedisPassword(),
                        authProperties.getRedisDb(),
                        authProperties.getRedisKeyPrefix(),
                        authProperties.getRedisTimeoutMs()
                ),
                new EmbeddedAdminStateStore.AuthMysqlConfig(
                        authProperties.getMysqlUrl(),
                        authProperties.getMysqlUser(),
                        authProperties.getMysqlPassword(),
                        authProperties.getMysqlQuery(),
                        authProperties.getMysqlPoolMinIdle(),
                        authProperties.getMysqlPoolMaxSize(),
                        authProperties.getMysqlPoolConnectionTimeoutMs(),
                        authProperties.getMysqlPoolIdleTimeoutMs(),
                        authProperties.getMysqlPoolMaxLifetimeMs()
                ),
                new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                        authProperties.getPostgresqlUrl(),
                        authProperties.getPostgresqlUser(),
                        authProperties.getPostgresqlPassword(),
                        authProperties.getPostgresqlQuery(),
                        authProperties.getPostgresqlPoolMinIdle(),
                        authProperties.getPostgresqlPoolMaxSize(),
                        authProperties.getPostgresqlPoolConnectionTimeoutMs(),
                        authProperties.getPostgresqlPoolIdleTimeoutMs(),
                        authProperties.getPostgresqlPoolMaxLifetimeMs()
                )
        );
    }

    private static EmbeddedAdminStateStore.ClusterConfig buildInitialClusterConfig(StartupContext context) {
        List<String> coreNodes = new ArrayList<>(context.clusterRoleProvider().coreEndpoints());
        if (coreNodes.isEmpty()) {
            coreNodes.add(context.clusterSettings().coreBindHost() + ":" + context.clusterSettings().coreBindPort());
        }
        return new EmbeddedAdminStateStore.ClusterConfig(
                coreNodes,
                List.of(),
                true,
                context.sharedMaxSubscribers()
        );
    }

    private static EmbeddedAdminStateStore.BridgeConfig buildInitialBridgeConfig(BridgeProperties bridgeProperties) {
        List<String> configuredTypes = splitCommaList(bridgeProperties.getTypes());
        boolean bridgeEnabled = bridgeProperties.isEnabled();
        return new EmbeddedAdminStateStore.BridgeConfig(
                bridgeEnabled,
                configuredTypes,
                splitCommaList(bridgeProperties.getTopicFilters()),
                bridgeProperties.isAsync(),
                bridgeProperties.getAsyncQueueCapacity(),
                bridgeProperties.getAsyncWorkerCount(),
                new EmbeddedAdminStateStore.BridgeKafkaConfig(
                        bridgeEnabled && configuredTypes.contains("kafka"),
                        bridgeProperties.getKafkaBootstrapServers(),
                        bridgeProperties.getKafkaTopic(),
                        splitCommaList(bridgeProperties.getKafkaSourceTopicFilters()),
                        bridgeProperties.getKafkaAcks(),
                        bridgeProperties.getKafkaClientId(),
                        bridgeProperties.getKafkaCompressionType()
                ),
                new EmbeddedAdminStateStore.BridgeRocketmqConfig(
                        bridgeEnabled && configuredTypes.contains("rocketmq"),
                        bridgeProperties.getRocketmqNameServer(),
                        bridgeProperties.getRocketmqProducerGroup(),
                        bridgeProperties.getRocketmqTopic(),
                        splitCommaList(bridgeProperties.getRocketmqSourceTopicFilters()),
                        bridgeProperties.isRocketmqSyncSend(),
                        bridgeProperties.getRocketmqTimeoutMs()
                ),
                new EmbeddedAdminStateStore.BridgeMysqlConfig(
                        bridgeEnabled && configuredTypes.contains("mysql"),
                        bridgeProperties.getMysqlDriver(),
                        bridgeProperties.getMysqlUrl(),
                        bridgeProperties.getMysqlUser(),
                        bridgeProperties.getMysqlPassword(),
                        bridgeProperties.getMysqlTable(),
                        splitCommaList(bridgeProperties.getMysqlSourceTopicFilters()),
                        bridgeProperties.isMysqlAutoCreateTable(),
                        bridgeProperties.getMysqlPoolMinIdle(),
                        bridgeProperties.getMysqlPoolMaxSize(),
                        bridgeProperties.getMysqlPoolConnectionTimeoutMs(),
                        bridgeProperties.getMysqlPoolIdleTimeoutMs(),
                        bridgeProperties.getMysqlPoolMaxLifetimeMs()
                )
        );
    }

    private static void applyRuntimeSecurityConfig(
            AuthProperties authProperties,
            AclProperties aclProperties,
            ReloadableAuthProvider reloadableAuthProvider,
            ReloadableAclAuthorizer reloadableAclAuthorizer,
            EmbeddedAdminStateStore.SecurityConfig securityConfig
    ) {
        if (securityConfig == null) {
            return;
        }
        int cacheMillis = (int) Math.max(0, Math.min(Integer.MAX_VALUE, securityConfig.cacheTtlMs()));
        synchronized (JmqxApplication.class) {
            authProperties.setCacheMillis(cacheMillis);
            aclProperties.setCacheMillis(cacheMillis);

            List<String> authChain = normalizePluginList(securityConfig.authChain());
            if (!securityConfig.authEnabled()) {
                authProperties.setChain("");
            } else {
                authProperties.setChain(String.join(",", authChain));
            }
            authProperties.setHttpMethod(securityConfig.authHttp().method());
            authProperties.setHttpUrl(securityConfig.authHttp().url());
            authProperties.setHttpHeaders(securityConfig.authHttp().headersText());
            authProperties.setHttpTlsEnabled(securityConfig.authHttp().tlsEnabled());
            authProperties.setHttpBodyTemplate(securityConfig.authHttp().bodyTemplate());
            authProperties.setHttpPoolSize(securityConfig.authHttp().poolSize());
            authProperties.setHttpRateLimitPerSecond(securityConfig.authHttp().rateLimitPerSecond());
            authProperties.setHttpRequestTimeoutMs(securityConfig.authHttp().requestTimeoutMs());
            authProperties.setHttpConnectTimeoutMs(securityConfig.authHttp().connectTimeoutMs());
            authProperties.setHttpPipelineCount(securityConfig.authHttp().pipelineCount());
            authProperties.setBuiltInDatabaseAccountType(securityConfig.authBuiltInDatabase().accountType());
            authProperties.setBuiltInDatabasePasswordHashAlgorithm(securityConfig.authBuiltInDatabase().passwordHashAlgorithm());
            authProperties.setBuiltInDatabaseSaltPosition(securityConfig.authBuiltInDatabase().saltPosition());
            authProperties.setRedisHost(securityConfig.authRedis().host());
            authProperties.setRedisPort(securityConfig.authRedis().port());
            authProperties.setRedisPassword(securityConfig.authRedis().password());
            authProperties.setRedisDb(securityConfig.authRedis().db());
            authProperties.setRedisKeyPrefix(securityConfig.authRedis().keyPrefix());
            authProperties.setRedisTimeoutMs(securityConfig.authRedis().timeoutMs());
            authProperties.setMysqlUrl(securityConfig.authMysql().url());
            authProperties.setMysqlUser(securityConfig.authMysql().user());
            authProperties.setMysqlPassword(securityConfig.authMysql().password());
            authProperties.setMysqlQuery(securityConfig.authMysql().query());
            authProperties.setMysqlPoolMinIdle(securityConfig.authMysql().poolMinIdle());
            authProperties.setMysqlPoolMaxSize(securityConfig.authMysql().poolMaxSize());
            authProperties.setMysqlPoolConnectionTimeoutMs(securityConfig.authMysql().poolConnectionTimeoutMs());
            authProperties.setMysqlPoolIdleTimeoutMs(securityConfig.authMysql().poolIdleTimeoutMs());
            authProperties.setMysqlPoolMaxLifetimeMs(securityConfig.authMysql().poolMaxLifetimeMs());

            authProperties.setPostgresqlUrl(securityConfig.authPostgresql().url());
            authProperties.setPostgresqlUser(securityConfig.authPostgresql().user());
            authProperties.setPostgresqlPassword(securityConfig.authPostgresql().password());
            authProperties.setPostgresqlQuery(securityConfig.authPostgresql().query());
            authProperties.setPostgresqlPoolMinIdle(securityConfig.authPostgresql().poolMinIdle());
            authProperties.setPostgresqlPoolMaxSize(securityConfig.authPostgresql().poolMaxSize());
            authProperties.setPostgresqlPoolConnectionTimeoutMs(securityConfig.authPostgresql().poolConnectionTimeoutMs());
            authProperties.setPostgresqlPoolIdleTimeoutMs(securityConfig.authPostgresql().poolIdleTimeoutMs());
            authProperties.setPostgresqlPoolMaxLifetimeMs(securityConfig.authPostgresql().poolMaxLifetimeMs());

            List<String> aclChain = normalizePluginList(securityConfig.aclChain());
            aclProperties.setDefaultAllow(securityConfig.aclDefaultAllow());
            aclProperties.setHttpUrl(securityConfig.aclHttp().url());
            aclProperties.setHttpTimeoutMs(securityConfig.aclHttp().timeoutMs());
            aclProperties.setHttpBodyTemplate(securityConfig.aclHttp().bodyTemplate());
            aclProperties.setFilePath(securityConfig.aclFile().path());
            aclProperties.setRedisHost(securityConfig.aclRedis().host());
            aclProperties.setRedisPort(securityConfig.aclRedis().port());
            aclProperties.setRedisPassword(securityConfig.aclRedis().password());
            aclProperties.setRedisDb(securityConfig.aclRedis().db());
            aclProperties.setRedisKeyPrefix(securityConfig.aclRedis().keyPrefix());
            aclProperties.setRedisTimeoutMs(securityConfig.aclRedis().timeoutMs());
            if (!securityConfig.aclEnabled() || aclChain.isEmpty()) {
                aclProperties.setChain("");
            } else {
                aclProperties.setChain(String.join(",", aclChain));
            }

            reloadableAuthProvider.setDelegate(AuthProviderFactory.create(authProperties));
            reloadableAclAuthorizer.setDelegate(AclAuthorizerFactory.create(aclProperties));
        }
    }

    private static void applyRuntimeBridgeConfig(
            BridgeProperties bridgeProperties,
            ReloadableMessageBridge reloadableMessageBridge,
            MqttBrokerMessageHandler brokerMessageHandler,
            EmbeddedAdminStateStore.BridgeConfig bridgeConfig
    ) {
        if (bridgeProperties == null || reloadableMessageBridge == null || bridgeConfig == null) {
            return;
        }
        synchronized (JmqxApplication.class) {
            List<String> activeTypes = new ArrayList<>();
            if (bridgeConfig.types().contains("kafka") && bridgeConfig.kafka().enabled()) {
                activeTypes.add("kafka");
            }
            if (bridgeConfig.types().contains("rocketmq") && bridgeConfig.rocketmq().enabled()) {
                activeTypes.add("rocketmq");
            }
            if (bridgeConfig.types().contains("mysql") && bridgeConfig.mysql().enabled()) {
                activeTypes.add("mysql");
            }
            boolean bridgeEnabled = bridgeConfig.enabled() && !activeTypes.isEmpty();

            bridgeProperties.setEnabled(bridgeEnabled);
            bridgeProperties.setTypes(String.join(",", activeTypes));
            bridgeProperties.setTopicFilters(String.join(",", bridgeConfig.topicFilters()));
            bridgeProperties.setAsync(bridgeConfig.asyncEnabled());
            bridgeProperties.setAsyncQueueCapacity(bridgeConfig.asyncQueueCapacity());
            bridgeProperties.setAsyncWorkerCount(bridgeConfig.asyncWorkerCount());
            bridgeProperties.setKafkaBootstrapServers(bridgeConfig.kafka().bootstrapServers());
            bridgeProperties.setKafkaTopic(bridgeConfig.kafka().topic());
            bridgeProperties.setKafkaSourceTopicFilters(String.join(",", bridgeConfig.kafka().sourceTopicFilters()));
            bridgeProperties.setKafkaAcks(bridgeConfig.kafka().acks());
            bridgeProperties.setKafkaClientId(bridgeConfig.kafka().clientId());
            bridgeProperties.setKafkaCompressionType(bridgeConfig.kafka().compressionType());
            bridgeProperties.setRocketmqNameServer(bridgeConfig.rocketmq().nameServer());
            bridgeProperties.setRocketmqProducerGroup(bridgeConfig.rocketmq().producerGroup());
            bridgeProperties.setRocketmqTopic(bridgeConfig.rocketmq().topic());
            bridgeProperties.setRocketmqSourceTopicFilters(String.join(",", bridgeConfig.rocketmq().sourceTopicFilters()));
            bridgeProperties.setRocketmqSyncSend(bridgeConfig.rocketmq().syncSend());
            bridgeProperties.setRocketmqTimeoutMs(bridgeConfig.rocketmq().timeoutMs());
            bridgeProperties.setMysqlDriver(bridgeConfig.mysql().driver());
            bridgeProperties.setMysqlUrl(bridgeConfig.mysql().url());
            bridgeProperties.setMysqlUser(bridgeConfig.mysql().user());
            bridgeProperties.setMysqlPassword(bridgeConfig.mysql().password());
            bridgeProperties.setMysqlTable(bridgeConfig.mysql().table());
            bridgeProperties.setMysqlSourceTopicFilters(String.join(",", bridgeConfig.mysql().sourceTopicFilters()));
            bridgeProperties.setMysqlAutoCreateTable(bridgeConfig.mysql().autoCreateTable());
            bridgeProperties.setMysqlPoolMinIdle(bridgeConfig.mysql().poolMinIdle());
            bridgeProperties.setMysqlPoolMaxSize(bridgeConfig.mysql().poolMaxSize());
            bridgeProperties.setMysqlPoolConnectionTimeoutMs(bridgeConfig.mysql().poolConnectionTimeoutMs());
            bridgeProperties.setMysqlPoolIdleTimeoutMs(bridgeConfig.mysql().poolIdleTimeoutMs());
            bridgeProperties.setMysqlPoolMaxLifetimeMs(bridgeConfig.mysql().poolMaxLifetimeMs());

            reloadableMessageBridge.setDelegate(MessageBridgeFactory.create(bridgeProperties));
            if (brokerMessageHandler != null) {
                brokerMessageHandler.updateBridgeSettings(
                        bridgeEnabled,
                        String.join(",", bridgeConfig.topicFilters())
                );
            }
        }
    }

    private static List<String> splitCommaList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        String[] items = raw.split(",");
        List<String> result = new ArrayList<>();
        for (String item : items) {
            if (item == null || item.isBlank()) {
                continue;
            }
            result.add(item.trim());
        }
        return result;
    }

    private static List<String> normalizePluginList(List<String> raw) {
        if (raw == null || raw.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String item : raw) {
            if (item == null || item.isBlank()) {
                continue;
            }
            String normalized = item.trim().toLowerCase();
            if ("allow_all".equals(normalized)) {
                continue;
            }
            result.add(normalized);
        }
        return result;
    }

    private static String getStringProperty(JmqxConfig config, String key, String defaultValue) {
        return config.getString(key, defaultValue);
    }

    private static String toDisplayHost(String host) {
        if (host == null || host.isBlank()) {
            return "127.0.0.1";
        }
        if ("0.0.0.0".equals(host) || "::".equals(host) || "::0".equals(host) || "*".equals(host)) {
            return "127.0.0.1";
        }
        return host;
    }

    private static String normalizeWebsocketPath(String path) {
        if (path == null || path.isBlank()) {
            return "/mqtt";
        }
        if (path.startsWith("/")) {
            return path;
        }
        return "/" + path;
    }

    private static int getIntProperty(JmqxConfig config, String key, int defaultValue) {
        return config.getInt(key, defaultValue);
    }

    private static boolean getBooleanProperty(JmqxConfig config, String key, boolean defaultValue) {
        return config.getBoolean(key, defaultValue);
    }

    private static String resolveLocalNodeIp() {
        String fromJvm = System.getProperty("jmqx.node.ip");
        if (fromJvm != null && !fromJvm.isBlank()) {
            return fromJvm.trim();
        }
        String fromEnv = System.getenv("JMQX_NODE_IP");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.trim();
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

    private static String safeJson(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }

    private static boolean isAdminDashboardClient(
            AdminAuthRuntime adminAuthRuntime,
            String clientId,
            String username,
            String password
    ) {
        if (adminAuthRuntime == null) {
            return false;
        }
        if (clientId == null || !clientId.startsWith("admin-")) {
            return false;
        }
        return adminAuthRuntime.matches(username, password);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    private static Set<String> getStringSetProperty(JmqxConfig config, String key) {
        return config.getStringSet(key);
    }

    /**
     * Immutable startup inputs shared by the staged bootstrap methods.
     */
    private record StartupContext(
            BrokerProperties brokerProperties,
            AuthProperties authProperties,
            AclProperties aclProperties,
            BridgeProperties bridgeProperties,
            RetainedStoreProperties retainedStoreProperties,
            String qos1InflightRocksdbPath,
            String qos2InflightRocksdbPath,
            boolean willPersistEnabled,
            String willRocksdbPath,
            int sharedMaxSubscribers,
            int sharedSlowThreshold,
            ClusterDispatchAsyncSettings clusterDispatchAsyncSettings,
            StorageAsyncSettings storageAsyncSettings,
            ClusterRoleProvider clusterRoleProvider,
            SessionRegistry sessionRegistry,
            SubscriptionRegistry subscriptionRegistry,
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            ClusterSettings clusterSettings,
            AdminSyncSettings adminSyncSettings,
            AdminAuthSettings adminAuthSettings,
            AdminPanelSettings adminPanelSettings
    ) {
    }

    /**
     * Async cluster message dispatch settings.
     */
    private record ClusterDispatchAsyncSettings(
            boolean enabled,
            int queueCapacity,
            int workerCount,
            int enqueueTimeoutMs
    ) {
    }

    /**
     * Async local store write settings for will and inflight persistence.
     */
    private record StorageAsyncSettings(
            boolean enabled,
            int queueCapacity,
            int workerCount,
            int enqueueTimeoutMs
    ) {
    }

    /**
     * Admin sync reporter settings.
     */
    private record AdminSyncSettings(
            boolean enabled,
            String url,
            String clusterId,
            String nodeIp,
            int connectTimeoutMs,
            int requestTimeoutMs,
            int metricsIntervalMs,
            int dashboardPublishIntervalMs
    ) {
    }

    private record AdminAuthSettings(
            String username,
            String password,
            String role
    ) {
    }

    /**
     * Embedded admin panel settings.
     */
    private record AdminPanelSettings(
            boolean enabled,
            String host,
            int port,
            String basePath,
            String backendUrl,
            boolean persistenceEnabled,
            String persistenceRocksdbPath
    ) {
    }

    /**
     * Group of broker endpoint servers.
     */
    private record EndpointServers(
            NettyMqttEndpointServer mqttServer,
            NettyMqttEndpointServer mqttTlsServer,
            NettyMqttEndpointServer mqttWebSocketServer,
            NettyMqttEndpointServer mqttWssServer
    ) {
    }

    /**
     * Full runtime component graph that must be released during shutdown.
     */
    private record RuntimeComponents(
            StartupContext startupContext,
            MetadataRuntime metadataRuntime,
            NettyClusterMessageTransport clusterMessageTransport,
            AutoCloseable clusterMessageDispatcherCloser,
            AutoCloseable storageAsyncExecutorCloser,
            ReloadableAuthProvider authProvider,
            MqttBrokerMessageHandler brokerMessageHandler,
            RetainedMessageStore retainedMessageStore,
            WillMessageStore willMessageStore,
            MessageBridge messageBridge,
            EndpointServers endpointServers,
            ScheduledExecutorService dashboardPublisher,
            AdminPanelServer adminPanelServer
    ) {
    }

    /**
     * Metadata gateway plus replicator lifecycle.
     */
    private record MetadataRuntime(MetadataCommandGateway gateway, MetadataReplicator replicator) {
        private MetadataRuntime {
            gateway = Objects.requireNonNull(gateway, "gateway");
            replicator = Objects.requireNonNull(replicator, "replicator");
        }
    }

    /**
     * Runtime stores created before the broker is assembled.
     */
    private record RuntimeStores(
            RetainedMessageStore retainedMessageStore,
            SharedAsyncStoreExecutor sharedAsyncStoreExecutor,
            Qos1InflightStore qos1InflightStore,
            Qos2InflightStore qos2InflightStore,
            WillMessageStore willMessageStore
    ) {
    }

    /**
     * Security-related runtime pieces used by both broker startup and admin updates.
     */
    private record SecurityRuntime(
            AdminStateRepository adminStateRepository,
            EmbeddedAdminStateStore.SecurityConfig initialSecurityConfig,
            ReloadableAuthProvider authProvider,
            ReloadableAclAuthorizer aclAuthorizer,
            AdminAuthRuntime adminAuthRuntime,
            RuntimeClientBlacklist clientBlacklist,
            ClientAuthenticator clientAuthenticator,
            BuiltInDatabaseUserService builtInDatabaseUserService
    ) {
    }

    /**
     * Bridge-related runtime state, including the current broker handler reference.
     */
    private record BridgeRuntime(
            ReloadableMessageBridge messageBridge,
            EmbeddedAdminStateStore.BridgeConfig initialBridgeConfig,
            AtomicReference<MqttBrokerMessageHandler> brokerMessageHandlerRef
    ) {
    }

    /**
     * Cluster message transport plus the dispatcher instance that should be closed on shutdown.
     */
    private record ClusterDispatchRuntime(
            NettyClusterMessageTransport transport,
            ClusterMessageDispatcher dispatcher,
            AutoCloseable dispatcherCloser
    ) {
    }

    /**
     * Composite lifecycle wrapper that starts and stops multiple metadata replicators together.
     */
    private static final class CompositeMetadataReplicator implements MetadataReplicator {
        private final MetadataReplicator first;
        private final MetadataReplicator second;
        private final AtomicBoolean started = new AtomicBoolean(false);

        private CompositeMetadataReplicator(MetadataReplicator first, MetadataReplicator second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void start() {
            if (!started.compareAndSet(false, true)) {
                return;
            }
            first.start();
            try {
                second.start();
            } catch (Exception exception) {
                started.set(false);
                first.stop();
                throw exception;
            }
        }

        @Override
        public void stop() {
            if (!started.compareAndSet(true, false)) {
                return;
            }
            try {
                second.stop();
            } finally {
                first.stop();
            }
        }
    }

    /**
     * Parses a global route snapshot topic key into normal/shared routing fields.
     */
    private record ParsedTopicKey(String topicFilter, String sharedGroup) {
        private static final String NORMAL_PREFIX = "n|";
        private static final String SHARED_PREFIX = "s|";

        private static ParsedTopicKey parse(String raw) {
            if (raw == null || raw.isBlank()) {
                return null;
            }
            if (raw.startsWith(NORMAL_PREFIX)) {
                String topicFilter = raw.substring(NORMAL_PREFIX.length());
                if (topicFilter.isBlank()) {
                    return null;
                }
                return new ParsedTopicKey(topicFilter, null);
            }
            if (raw.startsWith(SHARED_PREFIX)) {
                String remaining = raw.substring(SHARED_PREFIX.length());
                int idx = remaining.indexOf('|');
                if (idx <= 0 || idx >= remaining.length() - 1) {
                    return null;
                }
                String group = remaining.substring(0, idx);
                String topicFilter = remaining.substring(idx + 1);
                if (group.isBlank() || topicFilter.isBlank()) {
                    return null;
                }
                return new ParsedTopicKey(topicFilter, group);
            }
            return null;
        }
    }
}
