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
        properties.setType(config.getString("jmqx.auth.type", properties.getType()));
        properties.setChain(config.getString("jmqx.auth.chain", properties.getChain()));
        properties.setCacheMillis(config.getInt("jmqx.auth.cacheMillis", properties.getCacheMillis()));
        properties.setHttpUrl(config.getString("jmqx.auth.http.url", properties.getHttpUrl()));
        properties.setHttpTimeoutMs(config.getInt("jmqx.auth.http.timeoutMs", properties.getHttpTimeoutMs()));
        properties.setFilePath(config.getString("jmqx.auth.file.path", properties.getFilePath()));
        properties.setRedisHost(config.getString("jmqx.auth.redis.host", properties.getRedisHost()));
        properties.setRedisPort(config.getInt("jmqx.auth.redis.port", properties.getRedisPort()));
        properties.setRedisPassword(config.getString("jmqx.auth.redis.password", properties.getRedisPassword()));
        properties.setRedisDb(config.getInt("jmqx.auth.redis.db", properties.getRedisDb()));
        properties.setRedisKeyPrefix(config.getString("jmqx.auth.redis.keyPrefix", properties.getRedisKeyPrefix()));
        properties.setRedisTimeoutMs(config.getInt("jmqx.auth.redis.timeoutMs", properties.getRedisTimeoutMs()));
        properties.setDbDriver(config.getString("jmqx.auth.db.driver", properties.getDbDriver()));
        properties.setDbUrl(config.getString("jmqx.auth.db.url", properties.getDbUrl()));
        properties.setDbUser(config.getString("jmqx.auth.db.user", properties.getDbUser()));
        properties.setDbPassword(config.getString("jmqx.auth.db.password", properties.getDbPassword()));
        properties.setDbQuery(config.getString("jmqx.auth.db.query", properties.getDbQuery()));
        return properties;
    }

    public static AclProperties loadAclProperties(JmqxConfig config) {
        AclProperties properties = new AclProperties();
        properties.setType(config.getString("jmqx.acl.type", properties.getType()));
        properties.setDefaultAllow(config.getBoolean("jmqx.acl.defaultAllow", properties.isDefaultAllow()));
        properties.setCacheMillis(config.getInt("jmqx.acl.cacheMillis", properties.getCacheMillis()));
        properties.setHttpUrl(config.getString("jmqx.acl.http.url", properties.getHttpUrl()));
        properties.setHttpTimeoutMs(config.getInt("jmqx.acl.http.timeoutMs", properties.getHttpTimeoutMs()));
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
        if (!fromChain.isEmpty()) {
            return fromChain;
        }
        String type = authProperties.getType();
        if (type == null || type.isBlank()) {
            return List.of();
        }
        return normalizePluginList(List.of(type));
    }

    public static List<String> resolveAclChain(AclProperties aclProperties) {
        String type = aclProperties.getType();
        if (type == null || type.isBlank()) {
            return List.of();
        }
        return normalizePluginList(List.of(type));
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
}
