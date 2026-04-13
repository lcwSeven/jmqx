package com.jmqx;

import com.jmqx.admin.AdminReporter;
import com.jmqx.admin.HttpAdminReporter;
import com.jmqx.admin.embedded.AdminConfigCodec;
import com.jmqx.admin.embedded.AdminStateRepository;
import com.jmqx.admin.embedded.AdminPanelServer;
import com.jmqx.admin.embedded.BuiltInDatabaseUserService;
import com.jmqx.admin.embedded.EmbeddedAdminStateStore;
import com.jmqx.admin.embedded.RocksDbAdminStateStore;
import com.jmqx.acl.AclAuthorizerFactory;
import com.jmqx.acl.AclProperties;
import com.jmqx.acl.ReloadableAclAuthorizer;
import com.jmqx.auth.AuthProperties;
import com.jmqx.auth.AuthProviderFactory;
import com.jmqx.auth.AuthRequest;
import com.jmqx.auth.ReloadableAuthProvider;
import com.jmqx.broker.core.AsyncClusterMessageDispatcher;
import com.jmqx.broker.core.ClusterMessageDispatcher;
import com.jmqx.broker.core.MqttBrokerMessageHandler;
import com.jmqx.broker.ratelimit.BrokerRateLimitConfig;
import com.jmqx.bridge.BridgeProperties;
import com.jmqx.bridge.MessageBridge;
import com.jmqx.bridge.MessageBridgeFactory;
import com.jmqx.cluster.ClusterMetadataCommandApplier;
import com.jmqx.cluster.ClusterRoleProvider;
import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;
import com.jmqx.cluster.MetadataReplicator;
import com.jmqx.cluster.NodeRole;
import com.jmqx.cluster.StaticClusterRoleProvider;
import com.jmqx.cluster.core.MetadataSnapshot;
import com.jmqx.cluster.core.SofaJraftMetadataCommandGateway;
import com.jmqx.cluster.netty.NettyClusterMessageTransport;
import com.jmqx.cluster.netty.NettyMetadataCommandGateway;
import com.jmqx.cluster.netty.NettyMetadataCoreServer;
import com.jmqx.cluster.netty.NettyMetadataReplicantSyncClient;
import com.jmqx.common.BrokerProperties;
import com.jmqx.config.ClusterSettings;
import com.jmqx.config.JmqxConfig;
import com.jmqx.config.JmqxConfigMappers;
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.router.LocalSubscriptionRegistry;
import com.jmqx.router.SharedSubscriptionManager;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.router.global.DefaultGlobalSubscriptionRegistry;
import com.jmqx.router.global.GlobalSubscriptionRegistry;
import com.jmqx.session.LocalSessionRegistry;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.rocksdb.RocksDbRetainedMessageStore;
import com.jmqx.store.rocksdb.RocksDbQos1InflightStore;
import com.jmqx.store.rocksdb.RocksDbQos2InflightStore;
import com.jmqx.store.rocksdb.RocksDbWillMessageStore;
import com.jmqx.store.async.AsyncQos1InflightStore;
import com.jmqx.store.async.AsyncQos2InflightStore;
import com.jmqx.store.async.AsyncWillMessageStore;
import com.jmqx.store.qos.Qos1InflightStore;
import com.jmqx.store.qos.Qos2InflightStore;
import com.jmqx.store.retained.RetainedMessageStore;
import com.jmqx.store.retained.RetainedStoreProperties;
import com.jmqx.store.async.SharedAsyncStoreExecutor;
import com.jmqx.store.will.WillMessageStore;
import com.jmqx.transport.ConnectionMetrics;
import com.jmqx.transport.NettyMqttEndpointServer;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

/**
 * 应用启动装配入口，负责把 broker 和插件组装起来。
 *
 * @author liucaiwen
 * @date 2026/4/2
 */
public class JmqxApplication {
    public static void main(String[] args) throws InterruptedException {
        // 加载配置文件与 JVM 覆盖参数。
        JmqxConfig config = JmqxConfig.loadDefault();
        // 构建启动上下文，避免 main 方法堆积过多局部变量。
        StartupContext context = buildStartupContext(config);
        // 启动运行期组件（元数据复制、集群消息通道、Broker、网络端点）。
        RuntimeComponents runtimeComponents = startRuntime(context);
        // 注册优雅停机逻辑，保证组件按顺序释放资源。
        registerShutdownHook(runtimeComponents);
        // 输出启动摘要和关键生效配置，便于排查问题。
        printStartupSummary(context);
        // 阻塞主线程，保持进程持续运行直到收到退出信号。
        Thread.currentThread().join();
    }

    private static StartupContext buildStartupContext(JmqxConfig config) {
        BrokerProperties brokerProperties = JmqxConfigMappers.loadBrokerProperties(config);
        AuthProperties authProperties = JmqxConfigMappers.loadAuthProperties(config);
        AclProperties aclProperties = JmqxConfigMappers.loadAclProperties(config);
        BridgeProperties bridgeProperties = JmqxConfigMappers.loadBridgeProperties(config);
        RetainedStoreProperties retainedStoreProperties = JmqxConfigMappers.loadRetainedStoreProperties(config);
        String qos1InflightRocksdbPath = getStringProperty(
                config,
                "jmqx.qos1.inflight.rocksdb.path",
                "data/qos1-inflight-rocksdb"
        );
        String qos2InflightRocksdbPath = getStringProperty(
                config,
                "jmqx.qos2.inflight.rocksdb.path",
                "data/qos2-inflight-rocksdb"
        );
        boolean willPersistEnabled = getBooleanProperty(
                config,
                "jmqx.will.persist.enabled",
                true
        );
        String willRocksdbPath = getStringProperty(
                config,
                "jmqx.will.persist.rocksdb.path",
                "data/will-rocksdb"
        );
        AdminSyncSettings adminSyncSettings = loadAdminSyncSettings(config);
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
                adminPanelSettings
        );
    }

    private static RuntimeComponents startRuntime(StartupContext context) throws InterruptedException {
        // 0) 提前构建 retained store，供元数据投影器与 Broker 共用同一存储实例。
        RetainedMessageStore retainedMessageStore = buildRetainedMessageStore(context.retainedStoreProperties());
        SharedAsyncStoreExecutor sharedAsyncStoreExecutor = null;
        if (context.storageAsyncSettings().enabled()) {
            // Will/QoS1/QoS2 复用一个异步执行器，减少线程池数量，把线程预算留给连接与网络 IO。
            sharedAsyncStoreExecutor = new SharedAsyncStoreExecutor(
                    "jmqx-store-async",
                    context.storageAsyncSettings().queueCapacity(),
                    context.storageAsyncSettings().workerCount(),
                    context.storageAsyncSettings().enqueueTimeoutMs()
            );
        }
        Qos1InflightStore qos1InflightStore = buildQos1InflightStore(context, sharedAsyncStoreExecutor);
        Qos2InflightStore qos2InflightStore = buildQos2InflightStore(context, sharedAsyncStoreExecutor);
        WillMessageStore willMessageStore = buildWillMessageStore(context, sharedAsyncStoreExecutor);
        SharedSubscriptionManager sharedSubscriptionManager = new SharedSubscriptionManager(
                context.sharedMaxSubscribers(),
                context.sharedSlowThreshold()
        );
        ReloadableAuthProvider authProvider = new ReloadableAuthProvider(AuthProviderFactory.create(context.authProperties()));
        ReloadableAclAuthorizer aclAuthorizer = new ReloadableAclAuthorizer(AclAuthorizerFactory.create(context.aclProperties()));

        // 1) 构建并启动元数据运行时（CORE 负责写入与复制，REPLICANT 负责追平日志）。
        AdminStateRepository adminStateRepository = buildAdminStateRepository(context.adminPanelSettings());
        BuiltInDatabaseUserService builtInDatabaseUserService = new BuiltInDatabaseUserService();
        MetadataRuntime metadataRuntime = buildMetadataRuntime(
                context.clusterRoleProvider(),
                context.globalSubscriptionRegistry(),
                context.sessionRegistry(),
                retainedMessageStore,
                adminStateRepository,
                builtInDatabaseUserService,
                (clusterId, securityConfig) -> applyRuntimeSecurityConfig(
                        context.authProperties(),
                        context.aclProperties(),
                        authProvider,
                        aclAuthorizer,
                        securityConfig
                ),
                (clusterId, clusterConfig) -> applyRuntimeClusterConfig(
                        sharedSubscriptionManager,
                        context.sharedSlowThreshold(),
                        clusterConfig
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

        // 2) 初始化集群消息传输层，用于跨节点转发数据面 PUBLISH。
        NettyClusterMessageTransport clusterMessageTransport = new NettyClusterMessageTransport(
                context.clusterRoleProvider().nodeId(),
                context.clusterSettings().clusterMessageBindHost(),
                context.clusterSettings().clusterMessageBindPort(),
                context.clusterSettings().clusterRequestTimeoutMs(),
                context.clusterSettings().clusterNodeEndpoints()
        );
        // 将“按目标节点分发”映射到 Netty 集群传输层。
        ClusterMessageDispatcher syncClusterMessageDispatcher = (topic, payload, publishQos, targetPlans) -> {
            if (targetPlans == null || targetPlans.isEmpty()) {
                return;
            }
            targetPlans.forEach((targetNodeId, target) -> clusterMessageTransport.dispatch(
                    topic,
                    payload,
                    publishQos,
                    targetNodeId,
                    target.includeNormal(),
                    target.sharedGroups()
            ));
        };
        AutoCloseable clusterMessageDispatcherCloser = null;
        ClusterMessageDispatcher clusterMessageDispatcher;
        if (context.clusterDispatchAsyncSettings().enabled()) {
            AsyncClusterMessageDispatcher asyncDispatcher = new AsyncClusterMessageDispatcher(
                    syncClusterMessageDispatcher,
                    context.clusterDispatchAsyncSettings().queueCapacity(),
                    context.clusterDispatchAsyncSettings().workerCount(),
                    context.clusterDispatchAsyncSettings().enqueueTimeoutMs()
            );
            clusterMessageDispatcher = asyncDispatcher;
            clusterMessageDispatcherCloser = asyncDispatcher;
        } else {
            clusterMessageDispatcher = syncClusterMessageDispatcher;
        }

        // 3) 组装 Broker 依赖：共享订阅、保留消息、AUTH/ACL、桥接、连接指标、管理上报。
        ClientAuthenticator clientAuthenticator = (clientId, username, password) ->
                authProvider.authenticateResult(new AuthRequest(clientId, username, password));
        MessageBridge messageBridge = MessageBridgeFactory.create(context.bridgeProperties());
        ConnectionMetrics connectionMetrics = new ConnectionMetrics();
        AdminReporter adminReporter = buildAdminReporter(
                context.adminSyncSettings(),
                context.clusterRoleProvider().nodeId(),
                connectionMetrics
        );

        // 4) 构建 MQTT 核心处理器，统一处理 CONNECT/SUBSCRIBE/PUBLISH 等协议流转。
        MqttBrokerMessageHandler brokerMessageHandler = new MqttBrokerMessageHandler(
                context.sessionRegistry(),
                context.subscriptionRegistry(),
                retainedMessageStore,
                clientAuthenticator,
                aclAuthorizer,
                sharedSubscriptionManager,
                messageBridge,
                context.retainedStoreProperties().isRetainedEnabled(),
                qos1InflightStore,
                qos2InflightStore,
                willMessageStore,
                context.brokerProperties().getMaxWillPayloadBytes(),
                context.globalSubscriptionRegistry(),
                context.clusterRoleProvider().nodeId(),
                metadataRuntime.gateway(),
                clusterMessageDispatcher,
                adminReporter,
                context.adminSyncSettings().clusterId(),
                context.bridgeProperties().getTopicFilters(),
                context.brokerProperties().getMaxQos(),
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
        // 绑定集群入站消息回调：收到跨节点消息后交给 Broker 路由。
        clusterMessageTransport.setMessageConsumer(
                brokerMessageHandler::onClusterPublish
        );
        clusterMessageTransport.start();

        // 5) 启动 Dashboard 定时发布任务，输出节点与连接统计。
        ScheduledExecutorService dashboardPublisher = startDashboardPublisher(
                brokerMessageHandler,
                context.clusterRoleProvider().nodeId(),
                context.clusterRoleProvider().role().name(),
                context.adminSyncSettings().clusterId(),
                context.adminSyncSettings().nodeIp(),
                connectionMetrics,
                context.adminSyncSettings().dashboardPublishIntervalMs()
        );

        // 6) 启动管理面并注入运行时热更新回调（安全配置与集群配置）。
        AdminPanelServer adminPanelServer = startAdminPanelServer(
                context.adminPanelSettings(),
                context.adminSyncSettings().clusterId(),
                context.clusterRoleProvider().nodeId(),
                context.clusterRoleProvider().role().name(),
                context.sessionRegistry(),
                context.subscriptionRegistry(),
                connectionMetrics,
                adminStateRepository,
                builtInDatabaseUserService,
                buildInitialSecurityConfig(context.authProperties(), context.aclProperties()),
                buildInitialClusterConfig(context),
                (clusterId, securityConfig) -> metadataRuntime.gateway().submit(new MetadataCommand(
                        ClusterMetadataCommandApplier.ADMIN_SECURITY_NAMESPACE,
                        "upsert",
                        clusterId,
                        AdminConfigCodec.encodeSecurityConfigToString(securityConfig),
                        context.clusterRoleProvider().nodeId()
                )),
                (clusterId, clusterConfig) -> metadataRuntime.gateway().submit(new MetadataCommand(
                        ClusterMetadataCommandApplier.ADMIN_CLUSTER_NAMESPACE,
                        "upsert",
                        clusterId,
                        AdminConfigCodec.encodeClusterConfigToString(clusterConfig),
                        context.clusterRoleProvider().nodeId()
                )),
                new AdminPanelServer.BuiltInDatabaseUsersUpdater() {
                    @Override
                    public void upsert(String clusterId, EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config, String userId, String password, boolean superuser) {
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
                    public int importUsers(String clusterId, EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config, List<BuiltInDatabaseUserService.UserInput> users) {
                        int count = 0;
                        if (users == null) {
                            return 0;
                        }
                        for (BuiltInDatabaseUserService.UserInput user : users) {
                            if (user == null || user.userId() == null || user.userId().isBlank() || user.password() == null || user.password().isBlank()) {
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
                }
        );

        // 7) 启动 MQTT/MQTTS/WS/WSS 监听端点。
        EndpointServers endpointServers = startEndpointServers(context.brokerProperties(), brokerMessageHandler, connectionMetrics);

        // 8) 聚合运行组件，交由 shutdown hook 做统一释放。
        return new RuntimeComponents(
                context,
                metadataRuntime,
                clusterMessageTransport,
                clusterMessageDispatcherCloser,
                sharedAsyncStoreExecutor,
                authProvider,
                brokerMessageHandler,
                retainedMessageStore,
                willMessageStore,
                messageBridge,
                endpointServers,
                dashboardPublisher,
                adminPanelServer
        );
    }

    private static EndpointServers startEndpointServers(
            BrokerProperties brokerProperties,
            MqttBrokerMessageHandler brokerMessageHandler,
            ConnectionMetrics connectionMetrics
    ) throws InterruptedException {
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
        System.out.println("JMQX started on " + brokerProperties.getHost() + ":" + brokerProperties.getPort());
        if (brokerProperties.isMqttsEnabled()) {
            String mqttsHost = toDisplayHost(brokerProperties.getMqttsHost());
            System.out.println("JMQX mqtts: mqtts://" + mqttsHost + ":" + brokerProperties.getMqttsPort());
        }
        if (brokerProperties.isWebsocketEnabled()) {
            String wsHost = toDisplayHost(brokerProperties.getWebsocketHost());
            String wsPath = normalizeWebsocketPath(brokerProperties.getWebsocketPath());
            System.out.println("JMQX websocket: ws://" + wsHost + ":" + brokerProperties.getWebsocketPort() + wsPath);
        }
        if (brokerProperties.isWssEnabled()) {
            String wssHost = toDisplayHost(brokerProperties.getWssHost());
            String wssPath = normalizeWebsocketPath(brokerProperties.getWssPath());
            System.out.println("JMQX wss: wss://" + wssHost + ":" + brokerProperties.getWssPort() + wssPath);
        }
        System.out.println("AUTH chain: " + context.authProperties().getChain());
        System.out.println("ACL plugin: " + context.aclProperties().getType());
        System.out.println("BRIDGE enabled=" + context.bridgeProperties().isEnabled()
                + ", types=" + context.bridgeProperties().getTypes()
                + ", topicFilters=" + context.bridgeProperties().getTopicFilters());
        System.out.println("RATE_LIMIT clientIdEnabled=" + brokerProperties.isRateLimitClientIdEnabled()
                + ", clientIdPerSecond=" + brokerProperties.getRateLimitClientIdPerSecond()
                + ", ipEnabled=" + brokerProperties.isRateLimitIpEnabled()
                + ", ipPerSecond=" + brokerProperties.getRateLimitIpPerSecond()
                + ", connectEnabled=" + brokerProperties.isRateLimitConnectEnabled()
                + ", connectGlobalPerSecond=" + brokerProperties.getRateLimitConnectGlobalPerSecond()
                + ", connectIpPerSecond=" + brokerProperties.getRateLimitConnectIpPerSecond()
                + ", cleanupIntervalSeconds=" + brokerProperties.getRateLimitCleanupIntervalSeconds()
                + ", idleSeconds=" + brokerProperties.getRateLimitIdleSeconds());
        System.out.println("BROKER maxWillPayloadBytes=" + brokerProperties.getMaxWillPayloadBytes()
                + ", maxSubscriptionsPerClient=" + brokerProperties.getMaxSubscriptionsPerClient());
        System.out.println("RETAINED maxEntries=" + context.retainedStoreProperties().getMaxEntries()
                + ", maxBytes=" + context.retainedStoreProperties().getMaxBytes()
                + ", maxPayloadBytes=" + context.retainedStoreProperties().getMaxPayloadBytes()
                + ", rocksdbPath=" + context.retainedStoreProperties().getRocksdbPath()
                + ", enabled=" + context.retainedStoreProperties().isRetainedEnabled()
                + ", overflowStrategy=" + context.retainedStoreProperties().getOverflowStrategy());
        System.out.println("QOS1 inflight rocksdbPath=" + context.qos1InflightRocksdbPath());
        System.out.println("QOS2 inflight rocksdbPath=" + context.qos2InflightRocksdbPath());
        System.out.println("WILL persist enabled=" + context.willPersistEnabled()
                + ", rocksdbPath=" + context.willRocksdbPath());
        System.out.println("SHARED maxSubscribersPerGroup=" + context.sharedMaxSubscribers()
                + ", slowConsumerStrikeThreshold=" + context.sharedSlowThreshold());
        System.out.println("CLUSTER-DISPATCH asyncEnabled=" + context.clusterDispatchAsyncSettings().enabled()
                + ", queueCapacity=" + context.clusterDispatchAsyncSettings().queueCapacity()
                + ", workerCount=" + context.clusterDispatchAsyncSettings().workerCount()
                + ", enqueueTimeoutMs=" + context.clusterDispatchAsyncSettings().enqueueTimeoutMs());
        System.out.println("STORE-ASYNC enabled=" + context.storageAsyncSettings().enabled()
                + ", queueCapacity=" + context.storageAsyncSettings().queueCapacity()
                + ", workerCount=" + context.storageAsyncSettings().workerCount()
                + ", enqueueTimeoutMs=" + context.storageAsyncSettings().enqueueTimeoutMs());
        System.out.println("ADMIN-SYNC enabled=" + context.adminSyncSettings().enabled()
                + ", url=" + context.adminSyncSettings().url()
                + ", clusterId=" + context.adminSyncSettings().clusterId()
                + ", nodeIp=" + context.adminSyncSettings().nodeIp()
                + ", metricsIntervalMs=" + context.adminSyncSettings().metricsIntervalMs()
                + ", dashboardPublishIntervalMs=" + context.adminSyncSettings().dashboardPublishIntervalMs());
        System.out.println("ADMIN-PANEL enabled=" + context.adminPanelSettings().enabled()
                + ", host=" + context.adminPanelSettings().host()
                + ", port=" + context.adminPanelSettings().port()
                + ", path=" + context.adminPanelSettings().basePath()
                + ", backendUrl=" + context.adminPanelSettings().backendUrl());
        System.out.println("CLUSTER role=" + clusterRoleProvider.role()
                + ", nodeId=" + clusterRoleProvider.nodeId()
                + ", transport=netty"
                + ", coreEndpoints=" + clusterRoleProvider.coreEndpoints()
                + ", coreBindHost=" + clusterSettings.coreBindHost()
                + ", coreBindPort=" + clusterSettings.coreBindPort()
                + ", nettyRequestTimeoutMs=" + clusterSettings.clusterRequestTimeoutMs()
                + ", reconnectBackoffMs=" + clusterSettings.clusterReconnectBackoffMs()
                + ", ackBatchSize=" + clusterSettings.clusterAckBatchSize()
                + ", ackFlushIntervalMs=" + clusterSettings.clusterAckFlushIntervalMs()
                + ", replicantMaxInFlightEvents=" + clusterSettings.clusterReplicantMaxInFlightEvents()
                + ", replicantPushBatchSize=" + clusterSettings.clusterReplicantPushBatchSize()
                + ", nodeDownCleanupDelayMs=" + clusterSettings.clusterNodeDownCleanupDelayMs()
                + ", messageBindHost=" + clusterSettings.clusterMessageBindHost()
                + ", messageBindPort=" + clusterSettings.clusterMessageBindPort()
                + ", nodeEndpoints=" + clusterSettings.clusterNodeEndpoints()
                + ", replayMaxEvents=" + clusterSettings.clusterReplayMaxEvents()
                + ", raftGroupId=" + clusterSettings.raftGroupId()
                + ", raftServerId=" + clusterSettings.raftServerId()
                + ", raftInitialConf=" + clusterSettings.raftInitialConf()
                + ", raftDataPath=" + clusterSettings.raftDataPath());
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
        Qos2InflightStore store = new RocksDbQos2InflightStore(context.qos2InflightRocksdbPath());
        if (!context.storageAsyncSettings().enabled() || sharedAsyncStoreExecutor == null) {
            return store;
        }
        return new AsyncQos2InflightStore(store, sharedAsyncStoreExecutor);
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
            String nodeId,
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
                nodeId,
                nodeIp,
                connectionMetrics,
                settings.connectTimeoutMs(),
                settings.requestTimeoutMs(),
                settings.metricsIntervalMs()
        );
    }

    private static ScheduledExecutorService startDashboardPublisher(
            MqttBrokerMessageHandler brokerMessageHandler,
            String nodeId,
            String nodeRole,
            String clusterId,
            String configuredNodeIp,
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
                String payload = "{"
                        + "\"clusterId\":\"" + safeJson(safeClusterId) + "\","
                        + "\"nodeId\":\"" + safeJson(nodeId) + "\","
                        + "\"role\":\"" + safeJson(nodeRole) + "\","
                        + "\"nodeIp\":\"" + safeJson(safeNodeIp) + "\","
                        + "\"connections\":" + connectionMetrics.getActiveConnections() + ","
                        + "\"inboundBytes\":" + connectionMetrics.getInboundBytes() + ","
                        + "\"outboundBytes\":" + connectionMetrics.getOutboundBytes() + ","
                        + "\"timestamp\":" + System.currentTimeMillis()
                        + "}";
                brokerMessageHandler.publishSystemTopic(topic, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } catch (Exception ignored) {
            }
        }, 1500, Math.max(500, publishIntervalMs), TimeUnit.MILLISECONDS);
        return scheduler;
    }

    private static AdminPanelServer startAdminPanelServer(
            AdminPanelSettings settings,
            String clusterId,
            String nodeId,
            String nodeRole,
            SessionRegistry sessionRegistry,
            SubscriptionRegistry subscriptionRegistry,
            ConnectionMetrics connectionMetrics,
            AdminStateRepository stateRepository,
            BuiltInDatabaseUserService builtInDatabaseUserService,
            EmbeddedAdminStateStore.SecurityConfig initialSecurityConfig,
            EmbeddedAdminStateStore.ClusterConfig initialClusterConfig,
            AdminPanelServer.SecurityConfigUpdater securityConfigUpdater,
            AdminPanelServer.ClusterConfigUpdater clusterConfigUpdater,
            AdminPanelServer.BuiltInDatabaseUsersUpdater builtInDatabaseUsersUpdater
    ) {
        String panelNodeIp = resolveLocalNodeIp();
        AdminPanelServer server = new AdminPanelServer(
                settings.host(),
                settings.port(),
                settings.basePath(),
                settings.backendUrl(),
                clusterId,
                nodeId,
                panelNodeIp,
                nodeRole,
                sessionRegistry,
                subscriptionRegistry,
                connectionMetrics,
                stateRepository,
                initialSecurityConfig,
                initialClusterConfig,
                securityConfigUpdater,
                clusterConfigUpdater,
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

    private static MetadataRuntime buildMetadataRuntime(
            ClusterRoleProvider clusterRoleProvider,
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            SessionRegistry sessionRegistry,
            RetainedMessageStore retainedMessageStore,
            AdminStateRepository adminStateRepository,
            BuiltInDatabaseUserService builtInDatabaseUserService,
            JmqxMetadataProjectionHandlers.AdminSecurityConfigApplier adminSecurityConfigApplier,
            JmqxMetadataProjectionHandlers.AdminClusterConfigApplier adminClusterConfigApplier,
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
                sessionRegistry,
                retainedMessageStore,
                adminStateRepository,
                builtInDatabaseUserService,
                adminSecurityConfigApplier,
                adminClusterConfigApplier
        );
        ClusterMetadataCommandApplier commandApplier = new ClusterMetadataCommandApplier(
                clusterRoleProvider.nodeId(),
                handlers::applyRouteSubscriptionCommand,
                handlers::applyClientOnlineCommand,
                handlers::applyRetainedCommand,
                handlers::applyAdminSecurityConfigCommand,
                handlers::applyAdminClusterConfigCommand,
                handlers::applyBuiltInUserCommand
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
        boolean aclEnabled = !aclChain.isEmpty() && !"allow_all".equalsIgnoreCase(JmqxConfigMappers.firstOrEmpty(aclChain));
        long cacheTtlMs = Math.max(authProperties.getCacheMillis(), aclProperties.getCacheMillis());
        return new EmbeddedAdminStateStore.SecurityConfig(
                aclEnabled,
                aclChain,
                authEnabled,
                authChain,
                Math.max(cacheTtlMs, 0),
                new EmbeddedAdminStateStore.AuthHttpConfig(
                        authProperties.getHttpUrl(),
                        authProperties.getHttpTimeoutMs()
                ),
                new EmbeddedAdminStateStore.AuthFileConfig(
                        authProperties.getFilePath()
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
                if (authChain.isEmpty()) {
                    authChain = List.of("file");
                }
                authProperties.setChain(String.join(",", authChain));
            }
            authProperties.setHttpUrl(securityConfig.authHttp().url());
            authProperties.setHttpTimeoutMs(securityConfig.authHttp().timeoutMs());
            authProperties.setFilePath(securityConfig.authFile().path());
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
            if (!securityConfig.aclEnabled()) {
                aclProperties.setType("allow_all");
            } else {
                if (aclChain.isEmpty()) {
                    aclChain = List.of("file");
                }
                aclProperties.setType(JmqxConfigMappers.firstOrEmpty(aclChain));
            }

            reloadableAuthProvider.setDelegate(AuthProviderFactory.create(authProperties));
            reloadableAclAuthorizer.setDelegate(AclAuthorizerFactory.create(aclProperties));
        }
    }

    private static void applyRuntimeClusterConfig(
            SharedSubscriptionManager sharedSubscriptionManager,
            int slowConsumerStrikeThreshold,
            EmbeddedAdminStateStore.ClusterConfig clusterConfig
    ) {
        if (sharedSubscriptionManager == null || clusterConfig == null) {
            return;
        }
        sharedSubscriptionManager.reconfigure(
                clusterConfig.sharedSubscriptionMaxMembersPerGroup(),
                slowConsumerStrikeThreshold
        );
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
            result.add(item.trim().toLowerCase());
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
     * 启动期配置与基础组件上下文。
     *
     * @author liucaiwen
     * @date 2026/4/9
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
            AdminPanelSettings adminPanelSettings
    ) {
    }

    /**
     * 集群消息异步分发配置。
     *
     * @author liucaiwen
     * @date 2026/4/13
     */
    private record ClusterDispatchAsyncSettings(
            boolean enabled,
            int queueCapacity,
            int workerCount,
            int enqueueTimeoutMs
    ) {
    }

    /**
     * 本地持久化异步写配置（Will/QoS inflight）。
     */
    private record StorageAsyncSettings(
            boolean enabled,
            int queueCapacity,
            int workerCount,
            int enqueueTimeoutMs
    ) {
    }

    /**
     * 管理端上报配置。
     *
     * @author liucaiwen
     * @date 2026/4/10
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

    /**
     * 内嵌管理页面配置。
     *
     * @author liucaiwen
     * @date 2026/4/10
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
     * 网络端点服务集合。
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private record EndpointServers(
            NettyMqttEndpointServer mqttServer,
            NettyMqttEndpointServer mqttTlsServer,
            NettyMqttEndpointServer mqttWebSocketServer,
            NettyMqttEndpointServer mqttWssServer
    ) {
    }

    /**
     * 运行时组件集合。
     *
     * @author liucaiwen
     * @date 2026/4/9
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
     * 集群元数据运行时封装。
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private record MetadataRuntime(MetadataCommandGateway gateway, MetadataReplicator replicator) {
        private MetadataRuntime {
            gateway = Objects.requireNonNull(gateway, "gateway");
            replicator = Objects.requireNonNull(replicator, "replicator");
        }
    }

    /**
     * 组合复制生命周期控制器。
     * 用于同时启动/停止多个复制组件（例如 Raft 与 Netty 元数据服务）。
     *
     * @author liucaiwen
     * @date 2026/4/9
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
     * 解析全局路由主题键。
     *
     * @author liucaiwen
     * @date 2026/4/9
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
