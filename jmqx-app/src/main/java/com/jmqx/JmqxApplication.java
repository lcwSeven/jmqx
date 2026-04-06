package com.jmqx;

import com.jmqx.acl.AclAuthorizerFactory;
import com.jmqx.acl.AclProperties;
import com.jmqx.acl.ReloadableAclAuthorizer;
import com.jmqx.admin.AdminBackendLauncher;
import com.jmqx.admin.AdminProperties;
import com.jmqx.admin.RuntimeConfigService;
import com.jmqx.auth.AuthProperties;
import com.jmqx.auth.AuthProviderFactory;
import com.jmqx.auth.AuthRequest;
import com.jmqx.auth.ReloadableAuthProvider;
import com.jmqx.broker.SimpleBrokerMessageHandler;
import com.jmqx.cluster.ClusterCoordinator;
import com.jmqx.cluster.ClusterMessageBus;
import com.jmqx.cluster.ClusterProperties;
import com.jmqx.cluster.ClusterRole;
import com.jmqx.cluster.LocalClusterMessageBus;
import com.jmqx.cluster.NoopClusterReplicator;
import com.jmqx.cluster.ReloadableClusterReplicator;
import com.jmqx.common.BrokerProperties;
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.router.InMemorySubscriptionRegistry;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.session.InMemorySessionRegistry;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.InMemoryRetainedMessageStore;
import com.jmqx.store.RetainedMessageStore;
import com.jmqx.transport.ConnectionMetrics;
import com.jmqx.transport.NettyMqttServer;
import com.jmqx.transport.NettyMqttWebSocketServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class JmqxApplication {
    public static void main(String[] args) throws InterruptedException {
        Properties config = loadConfigProperties();
        BrokerProperties brokerProperties = loadBrokerProperties(config);
        AuthProperties authProperties = loadAuthProperties(config);
        AclProperties aclProperties = loadAclProperties(config);
        AdminProperties adminProperties = loadAdminProperties(config);
        ClusterProperties clusterProperties = loadClusterProperties(config);
        SessionRegistry sessionRegistry = new InMemorySessionRegistry();
        SubscriptionRegistry subscriptionRegistry = new InMemorySubscriptionRegistry();
        RetainedMessageStore retainedMessageStore = new InMemoryRetainedMessageStore();
        ReloadableAuthProvider authProvider = new ReloadableAuthProvider(AuthProviderFactory.create(authProperties));
        ClientAuthenticator clientAuthenticator = (clientId, username, password) ->
                authProvider.authenticate(new AuthRequest(clientId, username, password));
        ReloadableAclAuthorizer aclAuthorizer = new ReloadableAclAuthorizer(AclAuthorizerFactory.create(aclProperties));
        ConnectionMetrics connectionMetrics = new ConnectionMetrics();
        RuntimeConfigService runtimeConfigService = new RuntimeConfigService(
            authProperties,
            aclProperties,
            authProvider,
            aclAuthorizer
        );

        ReloadableClusterReplicator clusterReplicator = new ReloadableClusterReplicator(new NoopClusterReplicator());

        SimpleBrokerMessageHandler brokerMessageHandler = new SimpleBrokerMessageHandler(
                sessionRegistry,
                subscriptionRegistry,
                retainedMessageStore,
                clientAuthenticator,
                aclAuthorizer,
                clusterReplicator
        );
        ClusterCoordinator clusterCoordinator = null;
        if (clusterProperties.isEnabled()) {
            ClusterMessageBus messageBus = buildClusterMessageBus(clusterProperties);
            clusterCoordinator = new ClusterCoordinator(clusterProperties, messageBus, brokerMessageHandler);
            clusterReplicator.setDelegate(clusterCoordinator);
            clusterCoordinator.start();
        }

        NettyMqttServer mqttServer = new NettyMqttServer(brokerProperties, brokerMessageHandler, connectionMetrics);
        mqttServer.start();
        NettyMqttWebSocketServer mqttWebSocketServer = new NettyMqttWebSocketServer(
            brokerProperties,
            brokerMessageHandler,
            connectionMetrics
        );
        mqttWebSocketServer.start();

        AdminBackendLauncher adminBackendLauncher = null;
        if (adminProperties.isEnabled()) {
            adminBackendLauncher = new AdminBackendLauncher(
                adminProperties,
                connectionMetrics,
                runtimeConfigService,
                sessionRegistry,
                subscriptionRegistry
            );
            adminBackendLauncher.start();
        }

        AdminBackendLauncher finalAdminBackendLauncher = adminBackendLauncher;
        ClusterCoordinator finalClusterCoordinator = clusterCoordinator;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (finalAdminBackendLauncher != null) {
                finalAdminBackendLauncher.stop();
            }
            if (finalClusterCoordinator != null) {
                finalClusterCoordinator.stop();
            }
            mqttWebSocketServer.stop();
            mqttServer.stop();
        }));
        System.out.println("JMQX started on " + brokerProperties.getHost() + ":" + brokerProperties.getPort());
        if (brokerProperties.isWebsocketEnabled()) {
            String wsHost = toDisplayHost(brokerProperties.getWebsocketHost());
            String wsPath = normalizeWebsocketPath(brokerProperties.getWebsocketPath());
            System.out.println("JMQX websocket: ws://" + wsHost + ":" + brokerProperties.getWebsocketPort() + wsPath);
        }
        System.out.println("AUTH plugin: " + authProperties.getType());
        System.out.println("ACL plugin: " + aclProperties.getType());
        System.out.println("CLUSTER enabled=" + clusterProperties.isEnabled() + ", nodeId=" + clusterProperties.getNodeId()
            + ", role=" + clusterProperties.getRole() + ", busType=" + clusterProperties.getBusType());
        if (adminProperties.isEnabled()) {
            String adminDisplayHost = toDisplayHost(adminProperties.getHost());
            System.out.println("ADMIN panel: http://" + adminDisplayHost + ":" + adminProperties.getPort());
            System.out.println("ADMIN API: http://" + adminDisplayHost + ":" + adminProperties.getPort() + "/api/admin/status");
        }

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
        return properties;
    }

    private static AuthProperties loadAuthProperties(Properties config) {
        AuthProperties properties = new AuthProperties();
        properties.setType(getStringProperty(config, "jmqx.auth.type", properties.getType()));
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

    private static AdminProperties loadAdminProperties(Properties config) {
        AdminProperties properties = new AdminProperties();
        properties.setEnabled(getBooleanProperty(config, "jmqx.admin.enabled", properties.isEnabled()));
        properties.setHost(getStringProperty(config, "jmqx.admin.host", properties.getHost()));
        properties.setPort(getIntProperty(config, "jmqx.admin.port", properties.getPort()));
        return properties;
    }

    private static ClusterProperties loadClusterProperties(Properties config) {
        ClusterProperties properties = new ClusterProperties();
        properties.setEnabled(getBooleanProperty(config, "jmqx.cluster.enabled", properties.isEnabled()));
        properties.setNodeId(getStringProperty(config, "jmqx.cluster.nodeId", properties.getNodeId()));
        String role = getStringProperty(config, "jmqx.cluster.role", properties.getRole().name());
        try {
            properties.setRole(ClusterRole.valueOf(role.toUpperCase()));
        } catch (IllegalArgumentException ignored) {
            properties.setRole(ClusterRole.MASTER);
        }
        properties.setBusType(getStringProperty(config, "jmqx.cluster.busType", properties.getBusType()));
        properties.setSeedNodes(getStringProperty(config, "jmqx.cluster.seedNodes", properties.getSeedNodes()));
        return properties;
    }

    private static ClusterMessageBus buildClusterMessageBus(ClusterProperties properties) {
        // Stage-1 cluster implementation: local bus skeleton.
        // Future bus types (redis/kafka/nats/grpc) should be wired here.
        return new LocalClusterMessageBus();
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
            int value = Integer.parseInt(raw);
            if (legacyKey.endsWith("cacheSeconds")) {
                return Math.max(value, 0) * 1000;
            }
            return value;
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
}
