package com.jmqx.config;

import com.jmqx.acl.AclProperties;
import com.jmqx.auth.AuthProperties;
import com.jmqx.bridge.BridgeProperties;
import com.jmqx.common.BrokerProperties;
import com.jmqx.store.retained.RetainedOverflowStrategy;
import com.jmqx.store.retained.RetainedStoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * JMQX 业务配置对象映射器。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public final class JmqxConfigMappers {
    private JmqxConfigMappers() {
    }

    public static BrokerProperties loadBrokerProperties(JmqxConfig config) {
        BrokerProperties properties = new BrokerProperties();
        properties.setHost(config.getString("jmqx.broker.host", properties.getHost()));
        properties.setPort(config.getInt("jmqx.broker.port", properties.getPort()));
        properties.setMqttsEnabled(config.getBoolean("jmqx.broker.mqtts.enabled", properties.isMqttsEnabled()));
        properties.setMqttsHost(config.getString("jmqx.broker.mqtts.host", properties.getMqttsHost()));
        properties.setMqttsPort(config.getInt("jmqx.broker.mqtts.port", properties.getMqttsPort()));
        properties.setBossThreads(config.getInt("jmqx.broker.bossThreads", properties.getBossThreads()));
        properties.setWorkerThreads(config.getInt("jmqx.broker.workerThreads", properties.getWorkerThreads()));
        properties.setReaderIdleSeconds(config.getInt("jmqx.broker.readerIdleSeconds", properties.getReaderIdleSeconds()));
        properties.setMaxQos(config.getInt("jmqx.broker.maxQos", properties.getMaxQos()));
        properties.setMaxWillPayloadBytes(config.getInt("jmqx.broker.maxWillPayloadBytes", properties.getMaxWillPayloadBytes()));
        properties.setMaxSubscriptionsPerClient(config.getInt("jmqx.broker.maxSubscriptionsPerClient", properties.getMaxSubscriptionsPerClient()));
        properties.setWebsocketEnabled(config.getBoolean("jmqx.broker.websocket.enabled", properties.isWebsocketEnabled()));
        properties.setWebsocketHost(config.getString("jmqx.broker.websocket.host", properties.getWebsocketHost()));
        properties.setWebsocketPort(config.getInt("jmqx.broker.websocket.port", properties.getWebsocketPort()));
        properties.setWebsocketPath(config.getString("jmqx.broker.websocket.path", properties.getWebsocketPath()));
        properties.setWssEnabled(config.getBoolean("jmqx.broker.wss.enabled", properties.isWssEnabled()));
        properties.setWssHost(config.getString("jmqx.broker.wss.host", properties.getWssHost()));
        properties.setWssPort(config.getInt("jmqx.broker.wss.port", properties.getWssPort()));
        properties.setWssPath(config.getString("jmqx.broker.wss.path", properties.getWssPath()));
        properties.setTlsCertChainFile(config.getString("jmqx.broker.tls.certChainFile", properties.getTlsCertChainFile()));
        properties.setTlsPrivateKeyFile(config.getString("jmqx.broker.tls.privateKeyFile", properties.getTlsPrivateKeyFile()));
        properties.setTlsPrivateKeyPassword(config.getString("jmqx.broker.tls.privateKeyPassword", properties.getTlsPrivateKeyPassword()));
        properties.setRateLimitClientIdEnabled(config.getBoolean("jmqx.broker.rateLimit.clientId.enabled", properties.isRateLimitClientIdEnabled()));
        properties.setRateLimitClientIdPerSecond(config.getInt("jmqx.broker.rateLimit.clientId.perSecond", properties.getRateLimitClientIdPerSecond()));
        properties.setRateLimitIpEnabled(config.getBoolean("jmqx.broker.rateLimit.ip.enabled", properties.isRateLimitIpEnabled()));
        properties.setRateLimitIpPerSecond(config.getInt("jmqx.broker.rateLimit.ip.perSecond", properties.getRateLimitIpPerSecond()));
        properties.setRateLimitPublishStrategy(config.getString("jmqx.broker.rateLimit.publish.strategy", properties.getRateLimitPublishStrategy()));
        properties.setRateLimitConnectEnabled(config.getBoolean("jmqx.broker.rateLimit.connect.enabled", properties.isRateLimitConnectEnabled()));
        properties.setRateLimitConnectGlobalPerSecond(config.getInt("jmqx.broker.rateLimit.connect.globalPerSecond", properties.getRateLimitConnectGlobalPerSecond()));
        properties.setRateLimitConnectIpPerSecond(config.getInt("jmqx.broker.rateLimit.connect.ipPerSecond", properties.getRateLimitConnectIpPerSecond()));
        properties.setRateLimitConnectStrategy(config.getString("jmqx.broker.rateLimit.connect.strategy", properties.getRateLimitConnectStrategy()));
        properties.setRateLimitCleanupIntervalSeconds(config.getInt("jmqx.broker.rateLimit.cleanupIntervalSeconds", properties.getRateLimitCleanupIntervalSeconds()));
        properties.setRateLimitIdleSeconds(config.getInt("jmqx.broker.rateLimit.idleSeconds", properties.getRateLimitIdleSeconds()));
        return properties;
    }

    public static AuthProperties loadAuthProperties(JmqxConfig config) {
        AuthProperties properties = new AuthProperties();
        properties.setChain(config.getString("jmqx.auth.chain", properties.getChain()));
        properties.setCacheMillis(config.getInt("jmqx.auth.cacheMillis", properties.getCacheMillis()));
        properties.setHttpMethod(config.getString("jmqx.auth.http.method", properties.getHttpMethod()));
        properties.setHttpUrl(config.getString("jmqx.auth.http.url", properties.getHttpUrl()));
        properties.setHttpHeaders(config.getString("jmqx.auth.http.headers", properties.getHttpHeaders()));
        properties.setHttpTlsEnabled(config.getBoolean("jmqx.auth.http.tlsEnabled", properties.isHttpTlsEnabled()));
        properties.setHttpBodyTemplate(config.getString("jmqx.auth.http.bodyTemplate", properties.getHttpBodyTemplate()));
        properties.setHttpPoolSize(config.getInt("jmqx.auth.http.poolSize", properties.getHttpPoolSize()));
        properties.setHttpRateLimitPerSecond(config.getInt("jmqx.auth.http.rateLimitPerSecond", properties.getHttpRateLimitPerSecond()));
        properties.setHttpRequestTimeoutMs(config.getInt("jmqx.auth.http.requestTimeoutMs", config.getInt("jmqx.auth.http.timeoutMs", properties.getHttpRequestTimeoutMs())));
        properties.setHttpConnectTimeoutMs(config.getInt("jmqx.auth.http.connectTimeoutMs", properties.getHttpConnectTimeoutMs()));
        properties.setHttpPipelineCount(config.getInt("jmqx.auth.http.pipelineCount", properties.getHttpPipelineCount()));
        properties.setBuiltInDatabaseAccountType(config.getString("jmqx.auth.builtInDatabase.accountType", properties.getBuiltInDatabaseAccountType()));
        properties.setBuiltInDatabasePasswordHashAlgorithm(config.getString("jmqx.auth.builtInDatabase.passwordHashAlgorithm", properties.getBuiltInDatabasePasswordHashAlgorithm()));
        properties.setBuiltInDatabaseSaltPosition(config.getString("jmqx.auth.builtInDatabase.saltPosition", properties.getBuiltInDatabaseSaltPosition()));
        properties.setRedisHost(config.getString("jmqx.auth.redis.host", properties.getRedisHost()));
        properties.setRedisPort(config.getInt("jmqx.auth.redis.port", properties.getRedisPort()));
        properties.setRedisPassword(config.getString("jmqx.auth.redis.password", properties.getRedisPassword()));
        properties.setRedisDb(config.getInt("jmqx.auth.redis.db", properties.getRedisDb()));
        properties.setRedisKeyPrefix(config.getString("jmqx.auth.redis.keyPrefix", properties.getRedisKeyPrefix()));
        properties.setRedisTimeoutMs(config.getInt("jmqx.auth.redis.timeoutMs", properties.getRedisTimeoutMs()));
        properties.setMysqlUrl(config.getString("jmqx.auth.mysql.url", properties.getMysqlUrl()));
        properties.setMysqlUser(config.getString("jmqx.auth.mysql.user", properties.getMysqlUser()));
        properties.setMysqlPassword(config.getString("jmqx.auth.mysql.password", properties.getMysqlPassword()));
        properties.setMysqlQuery(config.getString("jmqx.auth.mysql.query", properties.getMysqlQuery()));
        properties.setMysqlPoolMinIdle(config.getInt("jmqx.auth.mysql.pool.minIdle", properties.getMysqlPoolMinIdle()));
        properties.setMysqlPoolMaxSize(config.getInt("jmqx.auth.mysql.pool.maxSize", properties.getMysqlPoolMaxSize()));
        properties.setMysqlPoolConnectionTimeoutMs(config.getLong("jmqx.auth.mysql.pool.connectionTimeoutMs", properties.getMysqlPoolConnectionTimeoutMs()));
        properties.setMysqlPoolIdleTimeoutMs(config.getLong("jmqx.auth.mysql.pool.idleTimeoutMs", properties.getMysqlPoolIdleTimeoutMs()));
        properties.setMysqlPoolMaxLifetimeMs(config.getLong("jmqx.auth.mysql.pool.maxLifetimeMs", properties.getMysqlPoolMaxLifetimeMs()));
        properties.setPostgresqlUrl(config.getString("jmqx.auth.postgresql.url", properties.getPostgresqlUrl()));
        properties.setPostgresqlUser(config.getString("jmqx.auth.postgresql.user", properties.getPostgresqlUser()));
        properties.setPostgresqlPassword(config.getString("jmqx.auth.postgresql.password", properties.getPostgresqlPassword()));
        properties.setPostgresqlQuery(config.getString("jmqx.auth.postgresql.query", properties.getPostgresqlQuery()));
        properties.setPostgresqlPoolMinIdle(config.getInt("jmqx.auth.postgresql.pool.minIdle", properties.getPostgresqlPoolMinIdle()));
        properties.setPostgresqlPoolMaxSize(config.getInt("jmqx.auth.postgresql.pool.maxSize", properties.getPostgresqlPoolMaxSize()));
        properties.setPostgresqlPoolConnectionTimeoutMs(config.getLong("jmqx.auth.postgresql.pool.connectionTimeoutMs", properties.getPostgresqlPoolConnectionTimeoutMs()));
        properties.setPostgresqlPoolIdleTimeoutMs(config.getLong("jmqx.auth.postgresql.pool.idleTimeoutMs", properties.getPostgresqlPoolIdleTimeoutMs()));
        properties.setPostgresqlPoolMaxLifetimeMs(config.getLong("jmqx.auth.postgresql.pool.maxLifetimeMs", properties.getPostgresqlPoolMaxLifetimeMs()));
        return properties;
    }

    public static AclProperties loadAclProperties(JmqxConfig config) {
        AclProperties properties = new AclProperties();
        properties.setChain(config.getString("jmqx.acl.chain", properties.getChain()));
        properties.setDefaultAllow(config.getBoolean("jmqx.acl.defaultAllow", properties.isDefaultAllow()));
        properties.setCacheMillis(config.getInt("jmqx.acl.cacheMillis", properties.getCacheMillis()));
        properties.setHttpUrl(config.getString("jmqx.acl.http.url", properties.getHttpUrl()));
        properties.setHttpTimeoutMs(config.getInt("jmqx.acl.http.timeoutMs", properties.getHttpTimeoutMs()));
        properties.setHttpBodyTemplate(config.getString("jmqx.acl.http.bodyTemplate", properties.getHttpBodyTemplate()));
        properties.setRedisHost(config.getString("jmqx.acl.redis.host", properties.getRedisHost()));
        properties.setRedisPort(config.getInt("jmqx.acl.redis.port", properties.getRedisPort()));
        properties.setRedisPassword(config.getString("jmqx.acl.redis.password", properties.getRedisPassword()));
        properties.setRedisDb(config.getInt("jmqx.acl.redis.db", properties.getRedisDb()));
        properties.setRedisKeyPrefix(config.getString("jmqx.acl.redis.keyPrefix", properties.getRedisKeyPrefix()));
        properties.setRedisTimeoutMs(config.getInt("jmqx.acl.redis.timeoutMs", properties.getRedisTimeoutMs()));
        properties.setFilePath(config.getString("jmqx.acl.file.path", properties.getFilePath()));
        return properties;
    }

    public static BridgeProperties loadBridgeProperties(JmqxConfig config) {
        BridgeProperties properties = new BridgeProperties();
        properties.setEnabled(config.getBoolean("jmqx.bridge.enabled", properties.isEnabled()));
        properties.setTypes(config.getString("jmqx.bridge.types", properties.getTypes()));
        properties.setTopicFilters(config.getString("jmqx.bridge.topicFilters", properties.getTopicFilters()));
        properties.setAsync(config.getBoolean("jmqx.bridge.async.enabled", properties.isAsync()));
        properties.setAsyncQueueCapacity(config.getInt("jmqx.bridge.async.queueCapacity", properties.getAsyncQueueCapacity()));
        properties.setAsyncWorkerCount(config.getInt("jmqx.bridge.async.workerCount", properties.getAsyncWorkerCount()));
        properties.setKafkaBootstrapServers(config.getString("jmqx.bridge.kafka.bootstrapServers", properties.getKafkaBootstrapServers()));
        properties.setKafkaTopic(config.getString("jmqx.bridge.kafka.topic", properties.getKafkaTopic()));
        properties.setKafkaSourceTopicFilters(config.getString("jmqx.bridge.kafka.sourceTopicFilters", properties.getKafkaSourceTopicFilters()));
        properties.setKafkaAcks(config.getString("jmqx.bridge.kafka.acks", properties.getKafkaAcks()));
        properties.setKafkaClientId(config.getString("jmqx.bridge.kafka.clientId", properties.getKafkaClientId()));
        properties.setKafkaCompressionType(config.getString("jmqx.bridge.kafka.compressionType", properties.getKafkaCompressionType()));
        properties.setRocketmqNameServer(config.getString("jmqx.bridge.rocketmq.nameServer", properties.getRocketmqNameServer()));
        properties.setRocketmqProducerGroup(config.getString("jmqx.bridge.rocketmq.producerGroup", properties.getRocketmqProducerGroup()));
        properties.setRocketmqTopic(config.getString("jmqx.bridge.rocketmq.topic", properties.getRocketmqTopic()));
        properties.setRocketmqSourceTopicFilters(config.getString("jmqx.bridge.rocketmq.sourceTopicFilters", properties.getRocketmqSourceTopicFilters()));
        properties.setRocketmqSyncSend(config.getBoolean("jmqx.bridge.rocketmq.syncSend", properties.isRocketmqSyncSend()));
        properties.setRocketmqTimeoutMs(config.getInt("jmqx.bridge.rocketmq.timeoutMs", properties.getRocketmqTimeoutMs()));
        properties.setMysqlDriver(config.getString("jmqx.bridge.mysql.driver", properties.getMysqlDriver()));
        properties.setMysqlUrl(config.getString("jmqx.bridge.mysql.url", properties.getMysqlUrl()));
        properties.setMysqlUser(config.getString("jmqx.bridge.mysql.user", properties.getMysqlUser()));
        properties.setMysqlPassword(config.getString("jmqx.bridge.mysql.password", properties.getMysqlPassword()));
        properties.setMysqlTable(config.getString("jmqx.bridge.mysql.table", properties.getMysqlTable()));
        properties.setMysqlSourceTopicFilters(config.getString("jmqx.bridge.mysql.sourceTopicFilters", properties.getMysqlSourceTopicFilters()));
        properties.setMysqlAutoCreateTable(config.getBoolean("jmqx.bridge.mysql.autoCreateTable", properties.isMysqlAutoCreateTable()));
        properties.setMysqlPoolMinIdle(config.getInt("jmqx.bridge.mysql.pool.minIdle", properties.getMysqlPoolMinIdle()));
        properties.setMysqlPoolMaxSize(config.getInt("jmqx.bridge.mysql.pool.maxSize", properties.getMysqlPoolMaxSize()));
        properties.setMysqlPoolConnectionTimeoutMs(config.getLong("jmqx.bridge.mysql.pool.connectionTimeoutMs", properties.getMysqlPoolConnectionTimeoutMs()));
        properties.setMysqlPoolIdleTimeoutMs(config.getLong("jmqx.bridge.mysql.pool.idleTimeoutMs", properties.getMysqlPoolIdleTimeoutMs()));
        properties.setMysqlPoolMaxLifetimeMs(config.getLong("jmqx.bridge.mysql.pool.maxLifetimeMs", properties.getMysqlPoolMaxLifetimeMs()));
        return properties;
    }

    public static RetainedStoreProperties loadRetainedStoreProperties(JmqxConfig config) {
        RetainedStoreProperties properties = new RetainedStoreProperties();
        properties.setRetainedEnabled(config.getBoolean("jmqx.retained.enabled", properties.isRetainedEnabled()));
        properties.setRocksdbPath(config.getString("jmqx.retained.rocksdb.path", properties.getRocksdbPath()));
        properties.setMaxEntries(config.getInt("jmqx.retained.maxEntries", properties.getMaxEntries()));
        properties.setMaxBytes(config.getLong("jmqx.retained.maxBytes", properties.getMaxBytes()));
        properties.setMaxPayloadBytes(config.getInt("jmqx.retained.maxPayloadBytes", properties.getMaxPayloadBytes()));
        String overflow = config.getString("jmqx.retained.overflowStrategy", properties.getOverflowStrategy().name());
        try {
            properties.setOverflowStrategy(RetainedOverflowStrategy.valueOf(overflow.toUpperCase()));
        } catch (IllegalArgumentException ignored) {
            properties.setOverflowStrategy(properties.getOverflowStrategy());
        }
        return properties;
    }

    public static ClusterSettings loadClusterSettings(JmqxConfig config) {
        String raftServerId = config.getString("jmqx.cluster.raft.serverId", "127.0.0.1:17800");
        return new ClusterSettings(
                config.getString("jmqx.cluster.core.bindHost", "0.0.0.0"),
                config.getInt("jmqx.cluster.core.port", 7800),
                config.getInt("jmqx.cluster.netty.requestTimeoutMs", 3000),
                config.getInt("jmqx.cluster.replay.maxEvents", 200000),
                config.getInt("jmqx.cluster.netty.reconnectBackoffMs", 1000),
                config.getInt("jmqx.cluster.netty.ackBatchSize", 64),
                config.getInt("jmqx.cluster.netty.ackFlushIntervalMs", 200),
                config.getInt("jmqx.cluster.netty.replicantMaxInFlightEvents", 2048),
                config.getInt("jmqx.cluster.netty.replicantPushBatchSize", 256),
                config.getInt("jmqx.cluster.nodeDownCleanupDelayMs", 15000),
                config.getString("jmqx.cluster.message.bindHost", "0.0.0.0"),
                config.getInt("jmqx.cluster.message.port", 7900),
                config.getStringMap("jmqx.cluster.nodeEndpoints"),
                config.getString("jmqx.cluster.raft.groupId", "jmqx-metadata"),
                raftServerId,
                config.getString("jmqx.cluster.raft.initialConf", raftServerId),
                config.getString("jmqx.cluster.raft.dataPath", "data/raft-metadata"),
                config.getInt("jmqx.cluster.raft.electionTimeoutMs", 1000),
                config.getInt("jmqx.cluster.raft.snapshotIntervalSecs", 30)
        );
    }

    public static List<String> resolveAuthChain(AuthProperties authProperties) {
        List<String> fromChain = normalizePluginList(splitCommaList(authProperties.getChain()));
        return filterRemovedAuthPlugins(filterAllowAll(fromChain));
    }

    public static List<String> resolveAclChain(AclProperties aclProperties) {
        List<String> fromChain = normalizePluginList(splitCommaList(aclProperties.getChain()));
        return filterAllowAll(fromChain);
    }

    public static String firstOrEmpty(List<String> values) {
        if (values == null || values.isEmpty() || values.get(0) == null) {
            return "";
        }
        return values.get(0);
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

    private static List<String> filterAllowAll(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String value : values) {
            if ("allow_all".equalsIgnoreCase(value)) {
                continue;
            }
            result.add(value);
        }
        return result;
    }

    private static List<String> filterRemovedAuthPlugins(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String value : values) {
            if ("file".equalsIgnoreCase(value)) {
                continue;
            }
            result.add(value);
        }
        return result;
    }
}
