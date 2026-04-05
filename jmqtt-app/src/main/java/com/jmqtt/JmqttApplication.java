package com.jmqtt;

import com.jmqtt.acl.AclAuthorizerFactory;
import com.jmqtt.acl.AclProperties;
import com.jmqtt.acl.ReloadableAclAuthorizer;
import com.jmqtt.admin.AdminBackendLauncher;
import com.jmqtt.admin.AdminProperties;
import com.jmqtt.admin.RuntimeConfigService;
import com.jmqtt.auth.AuthProperties;
import com.jmqtt.auth.AuthProviderFactory;
import com.jmqtt.auth.AuthRequest;
import com.jmqtt.auth.ReloadableAuthProvider;
import com.jmqtt.broker.SimpleBrokerMessageHandler;
import com.jmqtt.cluster.ClusterCoordinator;
import com.jmqtt.cluster.ClusterMessageBus;
import com.jmqtt.cluster.ClusterProperties;
import com.jmqtt.cluster.ClusterRole;
import com.jmqtt.cluster.LocalClusterMessageBus;
import com.jmqtt.cluster.NoopClusterReplicator;
import com.jmqtt.cluster.ReloadableClusterReplicator;
import com.jmqtt.common.BrokerProperties;
import com.jmqtt.protocol.ClientAuthenticator;
import com.jmqtt.router.InMemorySubscriptionRegistry;
import com.jmqtt.router.SubscriptionRegistry;
import com.jmqtt.session.InMemorySessionRegistry;
import com.jmqtt.session.SessionRegistry;
import com.jmqtt.store.InMemoryRetainedMessageStore;
import com.jmqtt.store.RetainedMessageStore;
import com.jmqtt.transport.ConnectionMetrics;
import com.jmqtt.transport.NettyMqttServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class JmqttApplication {
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
            mqttServer.stop();
        }));
        System.out.println("JMQTT started on " + brokerProperties.getHost() + ":" + brokerProperties.getPort());
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
        try (InputStream in = JmqttApplication.class.getClassLoader().getResourceAsStream("jmqtt.properties")) {
            if (in != null) {
                properties.load(in);
            }
        } catch (IOException ignored) {
        }
        return properties;
    }

    private static BrokerProperties loadBrokerProperties(Properties config) {
        BrokerProperties properties = new BrokerProperties();
        properties.setHost(getStringProperty(config, "jmqtt.broker.host", properties.getHost()));
        properties.setPort(getIntProperty(config, "jmqtt.broker.port", properties.getPort()));
        properties.setBossThreads(getIntProperty(config, "jmqtt.broker.bossThreads", properties.getBossThreads()));
        properties.setWorkerThreads(getIntProperty(config, "jmqtt.broker.workerThreads", properties.getWorkerThreads()));
        properties.setReaderIdleSeconds(getIntProperty(config, "jmqtt.broker.readerIdleSeconds", properties.getReaderIdleSeconds()));
        return properties;
    }

    private static AuthProperties loadAuthProperties(Properties config) {
        AuthProperties properties = new AuthProperties();
        properties.setType(getStringProperty(config, "jmqtt.auth.type", properties.getType()));
        properties.setCacheMillis(getIntProperty(
            config,
            "jmqtt.auth.cacheMillis",
            "jmqtt.auth.cacheSeconds",
            properties.getCacheMillis()
        ));

        properties.setHttpUrl(getStringProperty(config, "jmqtt.auth.http.url", properties.getHttpUrl()));
        properties.setHttpTimeoutMs(getIntProperty(config, "jmqtt.auth.http.timeoutMs", properties.getHttpTimeoutMs()));

        properties.setFilePath(getStringProperty(config, "jmqtt.auth.file.path", properties.getFilePath()));

        properties.setRedisHost(getStringProperty(config, "jmqtt.auth.redis.host", properties.getRedisHost()));
        properties.setRedisPort(getIntProperty(config, "jmqtt.auth.redis.port", properties.getRedisPort()));
        properties.setRedisPassword(getStringProperty(config, "jmqtt.auth.redis.password", properties.getRedisPassword()));
        properties.setRedisDb(getIntProperty(config, "jmqtt.auth.redis.db", properties.getRedisDb()));
        properties.setRedisKeyPrefix(getStringProperty(config, "jmqtt.auth.redis.keyPrefix", properties.getRedisKeyPrefix()));
        properties.setRedisTimeoutMs(getIntProperty(config, "jmqtt.auth.redis.timeoutMs", properties.getRedisTimeoutMs()));

        properties.setDbDriver(getStringProperty(config, "jmqtt.auth.db.driver", properties.getDbDriver()));
        properties.setDbUrl(getStringProperty(config, "jmqtt.auth.db.url", properties.getDbUrl()));
        properties.setDbUser(getStringProperty(config, "jmqtt.auth.db.user", properties.getDbUser()));
        properties.setDbPassword(getStringProperty(config, "jmqtt.auth.db.password", properties.getDbPassword()));
        properties.setDbQuery(getStringProperty(config, "jmqtt.auth.db.query", properties.getDbQuery()));
        return properties;
    }

    private static AclProperties loadAclProperties(Properties config) {
        AclProperties properties = new AclProperties();
        properties.setType(getStringProperty(config, "jmqtt.acl.type", properties.getType()));
        properties.setDefaultAllow(getBooleanProperty(config, "jmqtt.acl.defaultAllow", properties.isDefaultAllow()));
        properties.setCacheMillis(getIntProperty(
            config,
            "jmqtt.acl.cacheMillis",
            "jmqtt.acl.cacheSeconds",
            properties.getCacheMillis()
        ));

        properties.setHttpUrl(getStringProperty(config, "jmqtt.acl.http.url", properties.getHttpUrl()));
        properties.setHttpTimeoutMs(getIntProperty(config, "jmqtt.acl.http.timeoutMs", properties.getHttpTimeoutMs()));

        properties.setRedisHost(getStringProperty(config, "jmqtt.acl.redis.host", properties.getRedisHost()));
        properties.setRedisPort(getIntProperty(config, "jmqtt.acl.redis.port", properties.getRedisPort()));
        properties.setRedisPassword(getStringProperty(config, "jmqtt.acl.redis.password", properties.getRedisPassword()));
        properties.setRedisDb(getIntProperty(config, "jmqtt.acl.redis.db", properties.getRedisDb()));
        properties.setRedisKeyPrefix(getStringProperty(config, "jmqtt.acl.redis.keyPrefix", properties.getRedisKeyPrefix()));
        properties.setRedisTimeoutMs(getIntProperty(config, "jmqtt.acl.redis.timeoutMs", properties.getRedisTimeoutMs()));

        properties.setFilePath(getStringProperty(config, "jmqtt.acl.file.path", properties.getFilePath()));
        return properties;
    }

    private static AdminProperties loadAdminProperties(Properties config) {
        AdminProperties properties = new AdminProperties();
        properties.setEnabled(getBooleanProperty(config, "jmqtt.admin.enabled", properties.isEnabled()));
        properties.setHost(getStringProperty(config, "jmqtt.admin.host", properties.getHost()));
        properties.setPort(getIntProperty(config, "jmqtt.admin.port", properties.getPort()));
        return properties;
    }

    private static ClusterProperties loadClusterProperties(Properties config) {
        ClusterProperties properties = new ClusterProperties();
        properties.setEnabled(getBooleanProperty(config, "jmqtt.cluster.enabled", properties.isEnabled()));
        properties.setNodeId(getStringProperty(config, "jmqtt.cluster.nodeId", properties.getNodeId()));
        String role = getStringProperty(config, "jmqtt.cluster.role", properties.getRole().name());
        try {
            properties.setRole(ClusterRole.valueOf(role.toUpperCase()));
        } catch (IllegalArgumentException ignored) {
            properties.setRole(ClusterRole.MASTER);
        }
        properties.setBusType(getStringProperty(config, "jmqtt.cluster.busType", properties.getBusType()));
        properties.setSeedNodes(getStringProperty(config, "jmqtt.cluster.seedNodes", properties.getSeedNodes()));
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
