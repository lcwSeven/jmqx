package com.jmqx;

import com.jmqx.acl.AclAuthorizerFactory;
import com.jmqx.acl.AclProperties;
import com.jmqx.acl.ReloadableAclAuthorizer;
import com.jmqx.auth.AuthProperties;
import com.jmqx.auth.AuthProviderFactory;
import com.jmqx.auth.AuthRequest;
import com.jmqx.auth.ReloadableAuthProvider;
import com.jmqx.broker.MqttBrokerMessageHandler;
import com.jmqx.bridge.BridgeProperties;
import com.jmqx.bridge.MessageBridge;
import com.jmqx.bridge.MessageBridgeFactory;
import com.jmqx.cluster.ClusterRoleProvider;
import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;
import com.jmqx.cluster.MetadataReplicator;
import com.jmqx.cluster.NodeRole;
import com.jmqx.cluster.NoopMetadataCommandGateway;
import com.jmqx.cluster.NoopMetadataReplicator;
import com.jmqx.cluster.StaticClusterRoleProvider;
import com.jmqx.cluster.core.InMemoryMetadataCommandGateway;
import com.jmqx.cluster.core.MetadataSnapshot;
import com.jmqx.cluster.core.SofaJraftMetadataCommandGateway;
import com.jmqx.cluster.netty.NettyMetadataCommandGateway;
import com.jmqx.cluster.netty.NettyMetadataCoreServer;
import com.jmqx.cluster.netty.NettyMetadataReplicantSyncClient;
import com.jmqx.common.BrokerProperties;
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.router.LocalSubscriptionRegistry;
import com.jmqx.router.SharedSubscriptionManager;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.router.global.DefaultGlobalSubscriptionRegistry;
import com.jmqx.router.global.GlobalSubscriptionEvent;
import com.jmqx.router.global.GlobalSubscriptionRegistry;
import com.jmqx.session.InMemorySessionRegistry;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.RocksDbRetainedMessageStore;
import com.jmqx.store.RetainedMessageStore;
import com.jmqx.store.RetainedOverflowStrategy;
import com.jmqx.store.RetainedStoreProperties;
import com.jmqx.transport.ConnectionMetrics;
import com.jmqx.transport.NettyMqttEndpointServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 应用启动装配入口，负责把 broker 和插件组装起来。
 *
 * @author liucaiwen
 * @date 2026/4/2
 */
public class JmqxApplication {
    public static void main(String[] args) throws InterruptedException {
        Properties config = loadConfigProperties();
        BrokerProperties brokerProperties = loadBrokerProperties(config);
        AuthProperties authProperties = loadAuthProperties(config);
        AclProperties aclProperties = loadAclProperties(config);
        BridgeProperties bridgeProperties = loadBridgeProperties(config);
        RetainedStoreProperties retainedStoreProperties = loadRetainedStoreProperties(config);
        int sharedMaxSubscribers = getIntProperty(config, "jmqx.shared.maxSubscribersPerGroup", 1000);
        int sharedSlowThreshold = getIntProperty(config, "jmqx.shared.slowConsumerStrikeThreshold", 3);
        String nodeId = getStringProperty(config, "jmqx.node.id", "node-1");
        NodeRole nodeRole = NodeRole.from(getStringProperty(config, "jmqx.cluster.role", "core"), NodeRole.CORE);
        Set<String> coreEndpoints = getStringSetProperty(config, "jmqx.cluster.coreEndpoints");
        String coreBindHost = getStringProperty(config, "jmqx.cluster.core.bindHost", "0.0.0.0");
        int coreBindPort = getIntProperty(config, "jmqx.cluster.core.port", 7800);
        int clusterRequestTimeoutMs = getIntProperty(config, "jmqx.cluster.netty.requestTimeoutMs", 3000);
        int clusterReplayMaxEvents = getIntProperty(config, "jmqx.cluster.replay.maxEvents", 200000);
        int clusterReconnectBackoffMs = getIntProperty(config, "jmqx.cluster.netty.reconnectBackoffMs", 1000);
        int clusterAckBatchSize = getIntProperty(config, "jmqx.cluster.netty.ackBatchSize", 64);
        int clusterAckFlushIntervalMs = getIntProperty(config, "jmqx.cluster.netty.ackFlushIntervalMs", 200);
        int clusterReplicantMaxInFlightEvents = getIntProperty(
            config,
            "jmqx.cluster.netty.replicantMaxInFlightEvents",
            2048
        );
        int clusterReplicantPushBatchSize = getIntProperty(
            config,
            "jmqx.cluster.netty.replicantPushBatchSize",
            256
        );
        boolean raftEnabled = getBooleanProperty(config, "jmqx.cluster.raft.enabled", true);
        String raftGroupId = getStringProperty(config, "jmqx.cluster.raft.groupId", "jmqx-metadata");
        String raftServerId = getStringProperty(config, "jmqx.cluster.raft.serverId", "127.0.0.1:17800");
        String raftInitialConf = getStringProperty(config, "jmqx.cluster.raft.initialConf", raftServerId);
        String raftDataPath = getStringProperty(config, "jmqx.cluster.raft.dataPath", "data/raft-metadata");
        int raftElectionTimeoutMs = getIntProperty(config, "jmqx.cluster.raft.electionTimeoutMs", 1000);
        int raftSnapshotIntervalSecs = getIntProperty(config, "jmqx.cluster.raft.snapshotIntervalSecs", 30);
        ClusterRoleProvider clusterRoleProvider = new StaticClusterRoleProvider(nodeRole, nodeId, coreEndpoints);
        SessionRegistry sessionRegistry = new InMemorySessionRegistry();
        SubscriptionRegistry subscriptionRegistry = new LocalSubscriptionRegistry();
        GlobalSubscriptionRegistry globalSubscriptionRegistry = new DefaultGlobalSubscriptionRegistry();
        MetadataRuntime metadataRuntime = buildMetadataRuntime(
            clusterRoleProvider,
            globalSubscriptionRegistry,
            coreBindHost,
            coreBindPort,
            clusterRequestTimeoutMs,
            clusterReplayMaxEvents,
            raftEnabled,
            raftGroupId,
            raftServerId,
            raftInitialConf,
            raftDataPath,
            raftElectionTimeoutMs,
            raftSnapshotIntervalSecs,
            clusterReconnectBackoffMs,
            clusterAckBatchSize,
            clusterAckFlushIntervalMs,
            clusterReplicantMaxInFlightEvents,
            clusterReplicantPushBatchSize
        );
        metadataRuntime.replicator().start();
        SharedSubscriptionManager sharedSubscriptionManager = new SharedSubscriptionManager(
            sharedMaxSubscribers,
            sharedSlowThreshold
        );
        RetainedMessageStore retainedMessageStore = buildRetainedMessageStore(retainedStoreProperties);
        ReloadableAuthProvider authProvider = new ReloadableAuthProvider(AuthProviderFactory.create(authProperties));
        ClientAuthenticator clientAuthenticator = (clientId, username, password) ->
                authProvider.authenticate(new AuthRequest(clientId, username, password));
        ReloadableAclAuthorizer aclAuthorizer = new ReloadableAclAuthorizer(AclAuthorizerFactory.create(aclProperties));
        ConnectionMetrics connectionMetrics = new ConnectionMetrics();
        MessageBridge messageBridge = MessageBridgeFactory.create(bridgeProperties);

        MqttBrokerMessageHandler brokerMessageHandler = new MqttBrokerMessageHandler(
                sessionRegistry,
                subscriptionRegistry,
                retainedMessageStore,
                clientAuthenticator,
                aclAuthorizer,
                sharedSubscriptionManager,
                messageBridge,
                retainedStoreProperties.isRetainedEnabled(),
                globalSubscriptionRegistry,
                clusterRoleProvider.nodeId(),
                clusterRoleProvider,
                metadataRuntime.gateway()
        );

        // 先启动 MQTT TCP，再按需启动 WebSocket 接入。
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
        // 支持 MQTTS 的服务启动
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
        // 支持 ws 的服务启动
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
        // 支持 wss 的服务启动
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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            brokerMessageHandler.shutdown();
            retainedMessageStore.close();
            messageBridge.close();
            metadataRuntime.replicator().stop();
            mqttWssServer.stop();
            mqttWebSocketServer.stop();
            mqttTlsServer.stop();
            mqttServer.stop();
        }));
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
        System.out.println("AUTH plugin: " + authProperties.getType());
        System.out.println("ACL plugin: " + aclProperties.getType());
        System.out.println("BRIDGE enabled=" + bridgeProperties.isEnabled() + ", types=" + bridgeProperties.getTypes());
        System.out.println("RETAINED maxEntries=" + retainedStoreProperties.getMaxEntries()
            + ", maxBytes=" + retainedStoreProperties.getMaxBytes()
            + ", maxPayloadBytes=" + retainedStoreProperties.getMaxPayloadBytes()
            + ", rocksdbPath=" + retainedStoreProperties.getRocksdbPath()
            + ", enabled=" + retainedStoreProperties.isRetainedEnabled()
            + ", overflowStrategy=" + retainedStoreProperties.getOverflowStrategy());
        System.out.println("SHARED maxSubscribersPerGroup=" + sharedMaxSubscribers
            + ", slowConsumerStrikeThreshold=" + sharedSlowThreshold);
        System.out.println("CLUSTER role=" + clusterRoleProvider.role()
            + ", nodeId=" + clusterRoleProvider.nodeId()
            + ", transport=netty"
            + ", coreEndpoints=" + clusterRoleProvider.coreEndpoints()
            + ", coreBindHost=" + coreBindHost
            + ", coreBindPort=" + coreBindPort
            + ", nettyRequestTimeoutMs=" + clusterRequestTimeoutMs
            + ", reconnectBackoffMs=" + clusterReconnectBackoffMs
            + ", ackBatchSize=" + clusterAckBatchSize
            + ", ackFlushIntervalMs=" + clusterAckFlushIntervalMs
            + ", replicantMaxInFlightEvents=" + clusterReplicantMaxInFlightEvents
            + ", replicantPushBatchSize=" + clusterReplicantPushBatchSize
            + ", replayMaxEvents=" + clusterReplayMaxEvents
            + ", raftEnabled=" + raftEnabled
            + ", raftGroupId=" + raftGroupId
            + ", raftServerId=" + raftServerId
            + ", raftInitialConf=" + raftInitialConf
            + ", raftDataPath=" + raftDataPath);

        Thread.currentThread().join();
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

    private static MetadataRuntime buildMetadataRuntime(
        ClusterRoleProvider clusterRoleProvider,
        GlobalSubscriptionRegistry globalSubscriptionRegistry,
        String coreBindHost,
        int coreBindPort,
        int clusterRequestTimeoutMs,
        int clusterReplayMaxEvents,
        boolean raftEnabled,
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
        int clusterReplicantPushBatchSize
    ) {
        if (clusterRoleProvider.role() == NodeRole.CORE) {
            if (raftEnabled) {
                SofaJraftMetadataCommandGateway raftGateway = new SofaJraftMetadataCommandGateway(
                    raftGroupId,
                    raftServerId,
                    raftInitialConf,
                    raftDataPath,
                    raftElectionTimeoutMs,
                    raftSnapshotIntervalSecs,
                    clusterRequestTimeoutMs
                );
                raftGateway.registerApplier((logIndex, command) -> applyRouteSubscriptionCommand(
                    globalSubscriptionRegistry,
                    command,
                    logIndex
                ));
                NettyMetadataCoreServer coreServer = new NettyMetadataCoreServer(
                    coreBindHost,
                    coreBindPort,
                    raftGateway,
                    () -> buildRouteSnapshot(globalSubscriptionRegistry),
                    clusterReplayMaxEvents,
                    clusterReplicantMaxInFlightEvents,
                    clusterReplicantPushBatchSize
                );
                raftGateway.registerApplier(coreServer);
                return new MetadataRuntime(raftGateway, new CompositeMetadataReplicator(raftGateway, coreServer));
            }
            InMemoryMetadataCommandGateway gateway = new InMemoryMetadataCommandGateway();
            gateway.registerApplier((logIndex, command) -> applyRouteSubscriptionCommand(
                globalSubscriptionRegistry,
                command,
                logIndex
            ));
            NettyMetadataCoreServer coreServer = new NettyMetadataCoreServer(
                coreBindHost,
                coreBindPort,
                gateway,
                () -> buildRouteSnapshot(globalSubscriptionRegistry),
                clusterReplayMaxEvents,
                clusterReplicantMaxInFlightEvents,
                clusterReplicantPushBatchSize
            );
            gateway.registerApplier(coreServer);
            return new MetadataRuntime(gateway, coreServer);
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
            (logIndex, command) -> applyRouteSubscriptionCommand(globalSubscriptionRegistry, command, logIndex),
            globalSubscriptionRegistry::clear
        );
        return new MetadataRuntime(gateway, replicator);
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

    private static void applyRouteSubscriptionCommand(
        GlobalSubscriptionRegistry globalSubscriptionRegistry,
        MetadataCommand command,
        long logIndex
    ) {
        if (command == null || globalSubscriptionRegistry == null) {
            return;
        }
        if (!"route.subscription".equals(command.namespace())) {
            return;
        }
        String topicFilter = command.key();
        if (topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        String sourceNode = command.sourceNodeId();
        if (sourceNode == null || sourceNode.isBlank()) {
            return;
        }
        String sharedGroup = (command.value() == null || command.value().isBlank()) ? null : command.value();
        if ("register".equals(command.operation())) {
            globalSubscriptionRegistry.apply(GlobalSubscriptionEvent.register(logIndex, sourceNode, topicFilter, sharedGroup));
            return;
        }
        if ("unregister".equals(command.operation())) {
            globalSubscriptionRegistry.apply(GlobalSubscriptionEvent.unregister(logIndex, sourceNode, topicFilter, sharedGroup));
        }
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

    /**
     * Cluster metadata runtime holder.
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private record MetadataRuntime(MetadataCommandGateway gateway, MetadataReplicator replicator) {
        private MetadataRuntime {
            if (gateway == null) {
                gateway = NoopMetadataCommandGateway.INSTANCE;
            }
            if (replicator == null) {
                replicator = NoopMetadataReplicator.INSTANCE;
            }
        }
    }

    /**
     * 组合复制生命周期控制器。
     * 用于同时启动/停止多个复制组件（例如 Raft 与 gRPC 服务）。
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
     * Parse global route topic key.
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
