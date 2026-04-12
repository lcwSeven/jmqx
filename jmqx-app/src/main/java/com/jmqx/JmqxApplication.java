package com.jmqx;

import com.jmqx.admin.AdminReporter;
import com.jmqx.admin.HttpAdminReporter;
import com.jmqx.admin.embedded.AdminPanelServer;
import com.jmqx.admin.embedded.EmbeddedAdminStateStore;
import com.jmqx.acl.AclAuthorizerFactory;
import com.jmqx.acl.AclProperties;
import com.jmqx.acl.ReloadableAclAuthorizer;
import com.jmqx.auth.AuthProperties;
import com.jmqx.auth.AuthProviderFactory;
import com.jmqx.auth.AuthRequest;
import com.jmqx.auth.ReloadableAuthProvider;
import com.jmqx.broker.ClusterMessageDispatcher;
import com.jmqx.broker.MqttBrokerMessageHandler;
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
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.router.LocalSubscriptionRegistry;
import com.jmqx.router.SharedSubscriptionManager;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.router.global.DefaultGlobalSubscriptionRegistry;
import com.jmqx.router.global.GlobalSubscriptionRegistry;
import com.jmqx.session.LocalSessionRegistry;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.RocksDbRetainedMessageStore;
import com.jmqx.store.RetainedMessageStore;
import com.jmqx.store.RetainedOverflowStrategy;
import com.jmqx.store.RetainedStoreProperties;
import com.jmqx.transport.ConnectionMetrics;
import com.jmqx.transport.NettyMqttEndpointServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
        Properties config = loadConfigProperties();
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

    private static StartupContext buildStartupContext(Properties config) {
        BrokerProperties brokerProperties = loadBrokerProperties(config);
        AuthProperties authProperties = loadAuthProperties(config);
        AclProperties aclProperties = loadAclProperties(config);
        BridgeProperties bridgeProperties = loadBridgeProperties(config);
        RetainedStoreProperties retainedStoreProperties = loadRetainedStoreProperties(config);
        AdminSyncSettings adminSyncSettings = loadAdminSyncSettings(config);
        AdminPanelSettings adminPanelSettings = loadAdminPanelSettings(config);
        int sharedMaxSubscribers = getIntProperty(config, "jmqx.shared.maxSubscribersPerGroup", 1000);
        int sharedSlowThreshold = getIntProperty(config, "jmqx.shared.slowConsumerStrikeThreshold", 3);
        String nodeId = getStringProperty(config, "jmqx.node.id", "node-1");
        NodeRole nodeRole = NodeRole.from(getStringProperty(config, "jmqx.cluster.role", "core"), NodeRole.CORE);
        Set<String> coreEndpoints = getStringSetProperty(config, "jmqx.cluster.coreEndpoints");
        ClusterRoleProvider clusterRoleProvider = new StaticClusterRoleProvider(nodeRole, nodeId, coreEndpoints);
        SessionRegistry sessionRegistry = new LocalSessionRegistry();
        SubscriptionRegistry subscriptionRegistry = new LocalSubscriptionRegistry();
        GlobalSubscriptionRegistry globalSubscriptionRegistry = new DefaultGlobalSubscriptionRegistry();
        ClusterSettings clusterSettings = new ClusterSettings(
                getStringProperty(config, "jmqx.cluster.core.bindHost", "0.0.0.0"),
                getIntProperty(config, "jmqx.cluster.core.port", 7800),
                getIntProperty(config, "jmqx.cluster.netty.requestTimeoutMs", 3000),
                getIntProperty(config, "jmqx.cluster.replay.maxEvents", 200000),
                getIntProperty(config, "jmqx.cluster.netty.reconnectBackoffMs", 1000),
                getIntProperty(config, "jmqx.cluster.netty.ackBatchSize", 64),
                getIntProperty(config, "jmqx.cluster.netty.ackFlushIntervalMs", 200),
                getIntProperty(config, "jmqx.cluster.netty.replicantMaxInFlightEvents", 2048),
                getIntProperty(config, "jmqx.cluster.netty.replicantPushBatchSize", 256),
                getIntProperty(config, "jmqx.cluster.nodeDownCleanupDelayMs", 15000),
                getStringProperty(config, "jmqx.cluster.message.bindHost", "0.0.0.0"),
                getIntProperty(config, "jmqx.cluster.message.port", 7900),
                getStringMapProperty(config, "jmqx.cluster.nodeEndpoints"),
                getStringProperty(config, "jmqx.cluster.raft.groupId", "jmqx-metadata"),
                getStringProperty(config, "jmqx.cluster.raft.serverId", "127.0.0.1:17800"),
                getStringProperty(config, "jmqx.cluster.raft.initialConf", getStringProperty(config, "jmqx.cluster.raft.serverId", "127.0.0.1:17800")),
                getStringProperty(config, "jmqx.cluster.raft.dataPath", "data/raft-metadata"),
                getIntProperty(config, "jmqx.cluster.raft.electionTimeoutMs", 1000),
                getIntProperty(config, "jmqx.cluster.raft.snapshotIntervalSecs", 30)
        );
        return new StartupContext(
                brokerProperties,
                authProperties,
                aclProperties,
                bridgeProperties,
                retainedStoreProperties,
                sharedMaxSubscribers,
                sharedSlowThreshold,
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
        // 1) 构建并启动元数据运行时（CORE 负责写入与复制，REPLICANT 负责追平日志）。
        MetadataRuntime metadataRuntime = buildMetadataRuntime(
                context.clusterRoleProvider(),
                context.globalSubscriptionRegistry(),
                context.sessionRegistry(),
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
        ClusterMessageDispatcher clusterMessageDispatcher = (topic, payload, targetPlans) -> {
            if (targetPlans == null || targetPlans.isEmpty()) {
                return;
            }
            targetPlans.forEach((targetNodeId, target) -> clusterMessageTransport.dispatch(
                    topic,
                    payload,
                    targetNodeId,
                    target.includeNormal(),
                    target.sharedGroups()
            ));
        };

        // 3) 组装 Broker 依赖：共享订阅、保留消息、AUTH/ACL、桥接、连接指标、管理上报。
        SharedSubscriptionManager sharedSubscriptionManager = new SharedSubscriptionManager(
                context.sharedMaxSubscribers(),
                context.sharedSlowThreshold()
        );
        RetainedMessageStore retainedMessageStore = buildRetainedMessageStore(context.retainedStoreProperties());
        ReloadableAuthProvider authProvider = new ReloadableAuthProvider(AuthProviderFactory.create(context.authProperties()));
        ClientAuthenticator clientAuthenticator = (clientId, username, password) ->
                authProvider.authenticate(new AuthRequest(clientId, username, password));
        ReloadableAclAuthorizer aclAuthorizer = new ReloadableAclAuthorizer(AclAuthorizerFactory.create(context.aclProperties()));
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
                context.globalSubscriptionRegistry(),
                context.clusterRoleProvider().nodeId(),
                metadataRuntime.gateway(),
                clusterMessageDispatcher,
                adminReporter,
                context.adminSyncSettings().clusterId(),
                context.bridgeProperties().getTopicFilters()
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
                buildInitialSecurityConfig(context.authProperties(), context.aclProperties()),
                buildInitialClusterConfig(context),
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
                )
        );

        // 7) 启动 MQTT/MQTTS/WS/WSS 监听端点。
        EndpointServers endpointServers = startEndpointServers(context.brokerProperties(), brokerMessageHandler, connectionMetrics);

        // 8) 聚合运行组件，交由 shutdown hook 做统一释放。
        return new RuntimeComponents(
                context,
                metadataRuntime,
                clusterMessageTransport,
                brokerMessageHandler,
                retainedMessageStore,
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
            runtimeComponents.retainedMessageStore().close();
            runtimeComponents.messageBridge().close();
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
        System.out.println("AUTH plugin: " + context.authProperties().getType());
        System.out.println("ACL plugin: " + context.aclProperties().getType());
        System.out.println("BRIDGE enabled=" + context.bridgeProperties().isEnabled()
                + ", types=" + context.bridgeProperties().getTypes()
                + ", topicFilters=" + context.bridgeProperties().getTopicFilters());
        System.out.println("RETAINED maxEntries=" + context.retainedStoreProperties().getMaxEntries()
                + ", maxBytes=" + context.retainedStoreProperties().getMaxBytes()
                + ", maxPayloadBytes=" + context.retainedStoreProperties().getMaxPayloadBytes()
                + ", rocksdbPath=" + context.retainedStoreProperties().getRocksdbPath()
                + ", enabled=" + context.retainedStoreProperties().isRetainedEnabled()
                + ", overflowStrategy=" + context.retainedStoreProperties().getOverflowStrategy());
        System.out.println("SHARED maxSubscribersPerGroup=" + context.sharedMaxSubscribers()
                + ", slowConsumerStrikeThreshold=" + context.sharedSlowThreshold());
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

    private static Properties loadConfigProperties() {
        Properties properties = new Properties();
        try (InputStream in = JmqxApplication.class.getClassLoader().getResourceAsStream("jmqx.properties")) {
            if (in != null) {
                properties.load(in);
            }
        } catch (IOException ignored) {
        }
        return properties;
    }

    private static BrokerProperties loadBrokerProperties(Properties config) {
        BrokerProperties properties = new BrokerProperties();
        properties.setHost(getStringProperty(config, "jmqx.broker.host", properties.getHost()));
        properties.setPort(getIntProperty(config, "jmqx.broker.port", properties.getPort()));
        properties.setMqttsEnabled(getBooleanProperty(
                config,
                "jmqx.broker.mqtts.enabled",
                properties.isMqttsEnabled()
        ));
        properties.setMqttsHost(getStringProperty(
                config,
                "jmqx.broker.mqtts.host",
                properties.getMqttsHost()
        ));
        properties.setMqttsPort(getIntProperty(
                config,
                "jmqx.broker.mqtts.port",
                properties.getMqttsPort()
        ));
        properties.setBossThreads(getIntProperty(config, "jmqx.broker.bossThreads", properties.getBossThreads()));
        properties.setWorkerThreads(getIntProperty(config, "jmqx.broker.workerThreads", properties.getWorkerThreads()));
        properties.setReaderIdleSeconds(getIntProperty(config, "jmqx.broker.readerIdleSeconds", properties.getReaderIdleSeconds()));
        properties.setWebsocketEnabled(getBooleanProperty(
                config,
                "jmqx.broker.websocket.enabled",
                properties.isWebsocketEnabled()
        ));
        properties.setWebsocketHost(getStringProperty(
                config,
                "jmqx.broker.websocket.host",
                properties.getWebsocketHost()
        ));
        properties.setWebsocketPort(getIntProperty(
                config,
                "jmqx.broker.websocket.port",
                properties.getWebsocketPort()
        ));
        properties.setWebsocketPath(getStringProperty(
                config,
                "jmqx.broker.websocket.path",
                properties.getWebsocketPath()
        ));
        properties.setWssEnabled(getBooleanProperty(
                config,
                "jmqx.broker.wss.enabled",
                properties.isWssEnabled()
        ));
        properties.setWssHost(getStringProperty(
                config,
                "jmqx.broker.wss.host",
                properties.getWssHost()
        ));
        properties.setWssPort(getIntProperty(
                config,
                "jmqx.broker.wss.port",
                properties.getWssPort()
        ));
        properties.setWssPath(getStringProperty(
                config,
                "jmqx.broker.wss.path",
                properties.getWssPath()
        ));
        properties.setTlsCertChainFile(getStringProperty(
                config,
                "jmqx.broker.tls.certChainFile",
                properties.getTlsCertChainFile()
        ));
        properties.setTlsPrivateKeyFile(getStringProperty(
                config,
                "jmqx.broker.tls.privateKeyFile",
                properties.getTlsPrivateKeyFile()
        ));
        properties.setTlsPrivateKeyPassword(getStringProperty(
                config,
                "jmqx.broker.tls.privateKeyPassword",
                properties.getTlsPrivateKeyPassword()
        ));
        return properties;
    }

    private static AuthProperties loadAuthProperties(Properties config) {
        AuthProperties properties = new AuthProperties();
        properties.setType(getStringProperty(config, "jmqx.auth.type", properties.getType()));
        properties.setChain(getStringProperty(config, "jmqx.auth.chain", properties.getChain()));
        properties.setCacheMillis(getIntProperty(
                config,
                "jmqx.auth.cacheMillis",
                "jmqx.auth.cacheSeconds",
                properties.getCacheMillis()
        ));

        properties.setHttpUrl(getStringProperty(config, "jmqx.auth.http.url", properties.getHttpUrl()));
        properties.setHttpTimeoutMs(getIntProperty(config, "jmqx.auth.http.timeoutMs", properties.getHttpTimeoutMs()));

        properties.setFilePath(getStringProperty(config, "jmqx.auth.file.path", properties.getFilePath()));

        properties.setRedisHost(getStringProperty(config, "jmqx.auth.redis.host", properties.getRedisHost()));
        properties.setRedisPort(getIntProperty(config, "jmqx.auth.redis.port", properties.getRedisPort()));
        properties.setRedisPassword(getStringProperty(config, "jmqx.auth.redis.password", properties.getRedisPassword()));
        properties.setRedisDb(getIntProperty(config, "jmqx.auth.redis.db", properties.getRedisDb()));
        properties.setRedisKeyPrefix(getStringProperty(config, "jmqx.auth.redis.keyPrefix", properties.getRedisKeyPrefix()));
        properties.setRedisTimeoutMs(getIntProperty(config, "jmqx.auth.redis.timeoutMs", properties.getRedisTimeoutMs()));

        properties.setDbDriver(getStringProperty(config, "jmqx.auth.db.driver", properties.getDbDriver()));
        properties.setDbUrl(getStringProperty(config, "jmqx.auth.db.url", properties.getDbUrl()));
        properties.setDbUser(getStringProperty(config, "jmqx.auth.db.user", properties.getDbUser()));
        properties.setDbPassword(getStringProperty(config, "jmqx.auth.db.password", properties.getDbPassword()));
        properties.setDbQuery(getStringProperty(config, "jmqx.auth.db.query", properties.getDbQuery()));
        return properties;
    }

    private static AclProperties loadAclProperties(Properties config) {
        AclProperties properties = new AclProperties();
        properties.setType(getStringProperty(config, "jmqx.acl.type", properties.getType()));
        properties.setDefaultAllow(getBooleanProperty(config, "jmqx.acl.defaultAllow", properties.isDefaultAllow()));
        properties.setCacheMillis(getIntProperty(
                config,
                "jmqx.acl.cacheMillis",
                "jmqx.acl.cacheSeconds",
                properties.getCacheMillis()
        ));

        properties.setHttpUrl(getStringProperty(config, "jmqx.acl.http.url", properties.getHttpUrl()));
        properties.setHttpTimeoutMs(getIntProperty(config, "jmqx.acl.http.timeoutMs", properties.getHttpTimeoutMs()));

        properties.setRedisHost(getStringProperty(config, "jmqx.acl.redis.host", properties.getRedisHost()));
        properties.setRedisPort(getIntProperty(config, "jmqx.acl.redis.port", properties.getRedisPort()));
        properties.setRedisPassword(getStringProperty(config, "jmqx.acl.redis.password", properties.getRedisPassword()));
        properties.setRedisDb(getIntProperty(config, "jmqx.acl.redis.db", properties.getRedisDb()));
        properties.setRedisKeyPrefix(getStringProperty(config, "jmqx.acl.redis.keyPrefix", properties.getRedisKeyPrefix()));
        properties.setRedisTimeoutMs(getIntProperty(config, "jmqx.acl.redis.timeoutMs", properties.getRedisTimeoutMs()));

        properties.setFilePath(getStringProperty(config, "jmqx.acl.file.path", properties.getFilePath()));
        return properties;
    }

    private static BridgeProperties loadBridgeProperties(Properties config) {
        BridgeProperties properties = new BridgeProperties();
        properties.setEnabled(getBooleanProperty(config, "jmqx.bridge.enabled", properties.isEnabled()));
        properties.setTypes(getStringProperty(config, "jmqx.bridge.types", properties.getTypes()));
        properties.setTopicFilters(getStringProperty(config, "jmqx.bridge.topicFilters", properties.getTopicFilters()));
        properties.setAsync(getBooleanProperty(config, "jmqx.bridge.async", properties.isAsync()));
        properties.setAsyncQueueCapacity(getIntProperty(
                config,
                "jmqx.bridge.async.queueCapacity",
                properties.getAsyncQueueCapacity()
        ));
        properties.setAsyncWorkerCount(getIntProperty(
                config,
                "jmqx.bridge.async.workerCount",
                properties.getAsyncWorkerCount()
        ));

        properties.setKafkaBootstrapServers(getStringProperty(
                config,
                "jmqx.bridge.kafka.bootstrapServers",
                properties.getKafkaBootstrapServers()
        ));
        properties.setKafkaTopic(getStringProperty(
                config,
                "jmqx.bridge.kafka.topic",
                properties.getKafkaTopic()
        ));
        properties.setKafkaSourceTopicFilters(getStringProperty(
                config,
                "jmqx.bridge.kafka.sourceTopicFilters",
                properties.getKafkaSourceTopicFilters()
        ));
        properties.setKafkaAcks(getStringProperty(
                config,
                "jmqx.bridge.kafka.acks",
                properties.getKafkaAcks()
        ));
        properties.setKafkaClientId(getStringProperty(
                config,
                "jmqx.bridge.kafka.clientId",
                properties.getKafkaClientId()
        ));
        properties.setKafkaCompressionType(getStringProperty(
                config,
                "jmqx.bridge.kafka.compressionType",
                properties.getKafkaCompressionType()
        ));

        properties.setRocketmqNameServer(getStringProperty(
                config,
                "jmqx.bridge.rocketmq.nameServer",
                properties.getRocketmqNameServer()
        ));
        properties.setRocketmqProducerGroup(getStringProperty(
                config,
                "jmqx.bridge.rocketmq.producerGroup",
                properties.getRocketmqProducerGroup()
        ));
        properties.setRocketmqTopic(getStringProperty(
                config,
                "jmqx.bridge.rocketmq.topic",
                properties.getRocketmqTopic()
        ));
        properties.setRocketmqSourceTopicFilters(getStringProperty(
                config,
                "jmqx.bridge.rocketmq.sourceTopicFilters",
                properties.getRocketmqSourceTopicFilters()
        ));
        properties.setRocketmqSyncSend(getBooleanProperty(
                config,
                "jmqx.bridge.rocketmq.syncSend",
                properties.isRocketmqSyncSend()
        ));
        properties.setRocketmqTimeoutMs(getIntProperty(
                config,
                "jmqx.bridge.rocketmq.timeoutMs",
                properties.getRocketmqTimeoutMs()
        ));

        properties.setMysqlDriver(getStringProperty(
                config,
                "jmqx.bridge.mysql.driver",
                properties.getMysqlDriver()
        ));
        properties.setMysqlUrl(getStringProperty(
                config,
                "jmqx.bridge.mysql.url",
                properties.getMysqlUrl()
        ));
        properties.setMysqlUser(getStringProperty(
                config,
                "jmqx.bridge.mysql.user",
                properties.getMysqlUser()
        ));
        properties.setMysqlPassword(getStringProperty(
                config,
                "jmqx.bridge.mysql.password",
                properties.getMysqlPassword()
        ));
        properties.setMysqlTable(getStringProperty(
                config,
                "jmqx.bridge.mysql.table",
                properties.getMysqlTable()
        ));
        properties.setMysqlSourceTopicFilters(getStringProperty(
                config,
                "jmqx.bridge.mysql.sourceTopicFilters",
                properties.getMysqlSourceTopicFilters()
        ));
        properties.setMysqlAutoCreateTable(getBooleanProperty(
                config,
                "jmqx.bridge.mysql.autoCreateTable",
                properties.isMysqlAutoCreateTable()
        ));
        return properties;
    }

    private static RetainedStoreProperties loadRetainedStoreProperties(Properties config) {
        RetainedStoreProperties properties = new RetainedStoreProperties();
        properties.setRetainedEnabled(getBooleanProperty(
                config,
                "jmqx.retained.enabled",
                properties.isRetainedEnabled()
        ));
        properties.setRocksdbPath(getStringProperty(
                config,
                "jmqx.retained.rocksdb.path",
                properties.getRocksdbPath()
        ));
        properties.setMaxEntries(getIntProperty(
                config,
                "jmqx.retained.maxEntries",
                properties.getMaxEntries()
        ));
        properties.setMaxBytes(getLongProperty(
                config,
                "jmqx.retained.maxBytes",
                properties.getMaxBytes()
        ));
        properties.setMaxPayloadBytes(getIntProperty(
                config,
                "jmqx.retained.maxPayloadBytes",
                properties.getMaxPayloadBytes()
        ));
        properties.setOverflowStrategy(RetainedOverflowStrategy.parse(
                getStringProperty(config, "jmqx.retained.overflowStrategy", properties.getOverflowStrategy().name()),
                properties.getOverflowStrategy()
        ));
        return properties;
    }

    private static RetainedMessageStore buildRetainedMessageStore(RetainedStoreProperties properties) {
        return new RocksDbRetainedMessageStore(properties);
    }

    private static AdminSyncSettings loadAdminSyncSettings(Properties config) {
        return new AdminSyncSettings(
                getBooleanProperty(config, "jmqx.admin.enabled", false),
                getStringProperty(config, "jmqx.admin.url", "http://127.0.0.1:18080"),
                getStringProperty(config, "jmqx.admin.clusterId", "default"),
                getStringProperty(config, "jmqx.admin.nodeIp", ""),
                getIntProperty(config, "jmqx.admin.http.connectTimeoutMs", 1500),
                getIntProperty(config, "jmqx.admin.http.requestTimeoutMs", 3000),
                getIntProperty(config, "jmqx.admin.metrics.intervalMs", 5000),
                getIntProperty(config, "jmqx.admin.dashboard.publishIntervalMs", 2000)
        );
    }

    private static AdminPanelSettings loadAdminPanelSettings(Properties config) {
        return new AdminPanelSettings(
                getBooleanProperty(config, "jmqx.admin.panel.enabled", true),
                getStringProperty(config, "jmqx.admin.panel.host", "0.0.0.0"),
                getIntProperty(config, "jmqx.admin.panel.port", 18081),
                getStringProperty(config, "jmqx.admin.panel.basePath", "/admin"),
                getStringProperty(config, "jmqx.admin.panel.backendUrl", "http://127.0.0.1:18080")
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
            EmbeddedAdminStateStore.SecurityConfig initialSecurityConfig,
            EmbeddedAdminStateStore.ClusterConfig initialClusterConfig,
            AdminPanelServer.SecurityConfigUpdater securityConfigUpdater,
            AdminPanelServer.ClusterConfigUpdater clusterConfigUpdater
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
                initialSecurityConfig,
                initialClusterConfig,
                securityConfigUpdater,
                clusterConfigUpdater
        );
        if (settings.enabled()) {
            server.start();
        }
        return server;
    }

    private static MetadataRuntime buildMetadataRuntime(
            ClusterRoleProvider clusterRoleProvider,
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            SessionRegistry sessionRegistry,
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
                sessionRegistry
        );
        ClusterMetadataCommandApplier commandApplier = new ClusterMetadataCommandApplier(
                clusterRoleProvider.nodeId(),
                handlers::applyRouteSubscriptionCommand,
                handlers::applyClientOnlineCommand
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
        List<String> authChain = resolveAuthChain(authProperties);
        List<String> aclChain = resolveAclChain(aclProperties);
        boolean authEnabled = !authChain.isEmpty() && !"allow_all".equalsIgnoreCase(firstOrEmpty(authChain));
        boolean aclEnabled = !aclChain.isEmpty() && !"allow_all".equalsIgnoreCase(firstOrEmpty(aclChain));
        long cacheTtlMs = Math.max(authProperties.getCacheMillis(), aclProperties.getCacheMillis());
        return new EmbeddedAdminStateStore.SecurityConfig(
                aclEnabled,
                aclChain,
                authEnabled,
                authChain,
                Math.max(cacheTtlMs, 0)
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
                authProperties.setType("allow_all");
                authProperties.setChain("");
            } else {
                if (authChain.isEmpty()) {
                    authChain = List.of("file");
                }
                authProperties.setType(firstOrEmpty(authChain));
                authProperties.setChain(String.join(",", authChain));
            }

            List<String> aclChain = normalizePluginList(securityConfig.aclChain());
            if (!securityConfig.aclEnabled()) {
                aclProperties.setType("allow_all");
            } else {
                if (aclChain.isEmpty()) {
                    aclChain = List.of("file");
                }
                aclProperties.setType(firstOrEmpty(aclChain));
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

    private static List<String> resolveAuthChain(AuthProperties authProperties) {
        List<String> fromChain = normalizePluginList(splitCommaList(authProperties.getChain()));
        if (!fromChain.isEmpty()) {
            return fromChain;
        }
        String type = authProperties.getType();
        if (type == null || type.isBlank()) {
            return List.of("allow_all");
        }
        return List.of(type.trim().toLowerCase());
    }

    private static List<String> resolveAclChain(AclProperties aclProperties) {
        String type = aclProperties.getType();
        if (type == null || type.isBlank()) {
            return List.of("allow_all");
        }
        return List.of(type.trim().toLowerCase());
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

    private static String firstOrEmpty(List<String> values) {
        if (values == null || values.isEmpty() || values.get(0) == null) {
            return "";
        }
        return values.get(0);
    }

    private static String getStringProperty(Properties config, String key, String defaultValue) {
        String system = System.getProperty(key);
        if (system != null && !system.isBlank()) {
            return system;
        }
        String raw = config.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return raw;
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

    private static int getIntProperty(Properties config, String key, int defaultValue) {
        String raw = getStringProperty(config, key, Integer.toString(defaultValue));
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long getLongProperty(Properties config, String key, long defaultValue) {
        String raw = getStringProperty(config, key, Long.toString(defaultValue));
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    /**
     * 兼容历史配置键的整数读取。
     * 当新键缺失时回退到 legacyKey。
     */
    private static int getIntProperty(Properties config, String key, String legacyKey, int defaultValue) {
        String system = System.getProperty(key);
        if (system != null && !system.isBlank()) {
            try {
                return Integer.parseInt(system);
            } catch (NumberFormatException ignored) {
                return defaultValue;
            }
        }
        String raw = config.getProperty(key);
        if (raw == null || raw.isBlank()) {
            raw = config.getProperty(legacyKey);
        }
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static boolean getBooleanProperty(Properties config, String key, boolean defaultValue) {
        String raw = getStringProperty(config, key, Boolean.toString(defaultValue));
        if ("true".equalsIgnoreCase(raw) || "1".equals(raw) || "yes".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw) || "0".equals(raw) || "no".equalsIgnoreCase(raw)) {
            return false;
        }
        return defaultValue;
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

    private static Set<String> getStringSetProperty(Properties config, String key) {
        String raw = getStringProperty(config, key, "");
        if (raw.isBlank()) {
            return Set.of();
        }
        String[] values = raw.split(",");
        Set<String> result = new LinkedHashSet<>();
        for (String value : values) {
            if (value == null || value.isBlank()) {
                continue;
            }
            result.add(value.trim());
        }
        return result;
    }

    private static Map<String, String> getStringMapProperty(Properties config, String key) {
        String raw = getStringProperty(config, key, "");
        if (raw.isBlank()) {
            return Map.of();
        }
        String[] items = raw.split(",");
        Map<String, String> result = new java.util.LinkedHashMap<>();
        for (String item : items) {
            if (item == null || item.isBlank()) {
                continue;
            }
            int index = item.indexOf('=');
            if (index <= 0 || index >= item.length() - 1) {
                continue;
            }
            String mapKey = item.substring(0, index).trim();
            String mapValue = item.substring(index + 1).trim();
            if (mapKey.isEmpty() || mapValue.isEmpty()) {
                continue;
            }
            result.put(mapKey, mapValue);
        }
        return result;
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
            int sharedMaxSubscribers,
            int sharedSlowThreshold,
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
     * 集群相关配置集合。
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private record ClusterSettings(
            String coreBindHost,
            int coreBindPort,
            int clusterRequestTimeoutMs,
            int clusterReplayMaxEvents,
            int clusterReconnectBackoffMs,
            int clusterAckBatchSize,
            int clusterAckFlushIntervalMs,
            int clusterReplicantMaxInFlightEvents,
            int clusterReplicantPushBatchSize,
            int clusterNodeDownCleanupDelayMs,
            String clusterMessageBindHost,
            int clusterMessageBindPort,
            Map<String, String> clusterNodeEndpoints,
            String raftGroupId,
            String raftServerId,
            String raftInitialConf,
            String raftDataPath,
            int raftElectionTimeoutMs,
            int raftSnapshotIntervalSecs
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
            String backendUrl
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
            MqttBrokerMessageHandler brokerMessageHandler,
            RetainedMessageStore retainedMessageStore,
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
