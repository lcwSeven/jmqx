package com.jmqtt;

import com.jmqtt.acl.AclAuthorizer;
import com.jmqtt.acl.AclAuthorizerFactory;
import com.jmqtt.acl.AclProperties;
import com.jmqtt.auth.AuthProperties;
import com.jmqtt.auth.AuthProvider;
import com.jmqtt.auth.AuthProviderFactory;
import com.jmqtt.auth.AuthRequest;
import com.jmqtt.broker.BrokerMessageHandler;
import com.jmqtt.broker.SimpleBrokerMessageHandler;
import com.jmqtt.common.BrokerProperties;
import com.jmqtt.protocol.ClientAuthenticator;
import com.jmqtt.router.InMemorySubscriptionRegistry;
import com.jmqtt.router.SubscriptionRegistry;
import com.jmqtt.session.InMemorySessionRegistry;
import com.jmqtt.session.SessionRegistry;
import com.jmqtt.store.InMemoryRetainedMessageStore;
import com.jmqtt.store.RetainedMessageStore;
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
        SessionRegistry sessionRegistry = new InMemorySessionRegistry();
        SubscriptionRegistry subscriptionRegistry = new InMemorySubscriptionRegistry();
        RetainedMessageStore retainedMessageStore = new InMemoryRetainedMessageStore();
        AuthProvider authProvider = AuthProviderFactory.create(authProperties);
        ClientAuthenticator clientAuthenticator = (username, password) ->
            authProvider.authenticate(new AuthRequest(username, password));
        AclAuthorizer aclAuthorizer = AclAuthorizerFactory.create(aclProperties);

        BrokerMessageHandler brokerMessageHandler = new SimpleBrokerMessageHandler(
            sessionRegistry,
            subscriptionRegistry,
            retainedMessageStore,
            clientAuthenticator,
            aclAuthorizer
        );

        NettyMqttServer mqttServer = new NettyMqttServer(brokerProperties, brokerMessageHandler);
        mqttServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(mqttServer::stop));
        System.out.println("JMQTT started on " + brokerProperties.getHost() + ":" + brokerProperties.getPort());
        System.out.println("AUTH plugin: " + authProperties.getType());
        System.out.println("ACL plugin: " + aclProperties.getType());

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
        properties.setCacheSeconds(getIntProperty(config, "jmqtt.auth.cacheSeconds", properties.getCacheSeconds()));

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
        properties.setCacheSeconds(getIntProperty(config, "jmqtt.acl.cacheSeconds", properties.getCacheSeconds()));

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

    private static int getIntProperty(Properties config, String key, int defaultValue) {
        String raw = getStringProperty(config, key, Integer.toString(defaultValue));
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
}
