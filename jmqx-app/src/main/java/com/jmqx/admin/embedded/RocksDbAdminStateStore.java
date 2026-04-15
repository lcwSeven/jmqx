package com.jmqx.admin.embedded;

import com.jmqx.common.logging.ClientTraceManager;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 RocksDB 的管理端状态仓储。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public class RocksDbAdminStateStore implements AdminStateRepository {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger LOG = Logger.getLogger(RocksDbAdminStateStore.class.getName());

    private static final byte[] CLUSTER_SUMMARY_PREFIX = "cluster-summary:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLUSTER_CONFIG_PREFIX = "cluster-config:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SECURITY_CONFIG_PREFIX = "security-config:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BRIDGE_CONFIG_PREFIX = "bridge-config:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NODE_METRICS_PREFIX = "node-metrics:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BLACKLIST_PREFIX = "blacklist:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLIENT_TRACE_PREFIX = "client-trace:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AUDIT_LOG_PREFIX = "audit-log:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ADMIN_AUTH_CONFIG_KEY = "admin-auth-config".getBytes(StandardCharsets.UTF_8);

    private final EmbeddedAdminStateStore memoryFallback = new EmbeddedAdminStateStore();
    private final Options options;
    private final RocksDB db;

    public RocksDbAdminStateStore(String dbPath) {
        String path = normalizePath(dbPath);
        ensureParentDirectory(path);
        try {
            this.options = new Options().setCreateIfMissing(true);
            this.db = RocksDB.open(options, path);
            replayPersistedState();
        } catch (RocksDBException exception) {
            throw new IllegalStateException("open admin-state rocksdb failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public AdminAuthRuntime.Config getAdminAuthConfig() {
        return memoryFallback.getAdminAuthConfig();
    }

    @Override
    public void setAdminAuthConfig(AdminAuthRuntime.Config adminAuthConfig) {
        memoryFallback.setAdminAuthConfig(adminAuthConfig);
        put(ADMIN_AUTH_CONFIG_KEY, encodeAdminAuthConfig(memoryFallback.getAdminAuthConfig()), "setAdminAuthConfig");
    }

    @Override
    public boolean hasAdminAuthConfig() {
        return memoryFallback.hasAdminAuthConfig();
    }

    @Override
    public EmbeddedAdminStateStore.ClusterSummary createCluster(String clusterId, String displayName, String seedCoreNode) {
        EmbeddedAdminStateStore.ClusterSummary summary = memoryFallback.createCluster(clusterId, displayName, seedCoreNode);
        put(key(CLUSTER_SUMMARY_PREFIX, summary.clusterId()), encodeClusterSummary(summary), "createCluster");
        return summary;
    }

    @Override
    public List<EmbeddedAdminStateStore.ClusterSummary> listClusters() {
        return memoryFallback.listClusters();
    }

    @Override
    public EmbeddedAdminStateStore.ClusterSummary getClusterSummary(String clusterId) {
        return memoryFallback.getClusterSummary(clusterId);
    }

    @Override
    public EmbeddedAdminStateStore.ClusterConfig getClusterConfig(String clusterId) {
        return memoryFallback.getClusterConfig(clusterId);
    }

    @Override
    public void setClusterConfig(String clusterId, EmbeddedAdminStateStore.ClusterConfig clusterConfig) {
        memoryFallback.setClusterConfig(clusterId, clusterConfig);
        put(key(CLUSTER_CONFIG_PREFIX, normalize(clusterId)), encodeClusterConfig(clusterConfig), "setClusterConfig");
    }

    @Override
    public boolean hasClusterConfig(String clusterId) {
        return memoryFallback.hasClusterConfig(clusterId);
    }

    @Override
    public EmbeddedAdminStateStore.SecurityConfig getSecurityConfig(String clusterId) {
        return memoryFallback.getSecurityConfig(clusterId);
    }

    @Override
    public void setSecurityConfig(String clusterId, EmbeddedAdminStateStore.SecurityConfig securityConfig) {
        memoryFallback.setSecurityConfig(clusterId, securityConfig);
        put(key(SECURITY_CONFIG_PREFIX, normalize(clusterId)), encodeSecurityConfig(securityConfig), "setSecurityConfig");
    }

    @Override
    public boolean hasSecurityConfig(String clusterId) {
        return memoryFallback.hasSecurityConfig(clusterId);
    }

    @Override
    public EmbeddedAdminStateStore.BridgeConfig getBridgeConfig(String clusterId) {
        return memoryFallback.getBridgeConfig(clusterId);
    }

    @Override
    public void setBridgeConfig(String clusterId, EmbeddedAdminStateStore.BridgeConfig bridgeConfig) {
        memoryFallback.setBridgeConfig(clusterId, bridgeConfig);
        put(key(BRIDGE_CONFIG_PREFIX, normalize(clusterId)), encodeBridgeConfig(bridgeConfig), "setBridgeConfig");
    }

    @Override
    public boolean hasBridgeConfig(String clusterId) {
        return memoryFallback.hasBridgeConfig(clusterId);
    }

    @Override
    public EmbeddedAdminStateStore.ClusterFullConfig getFullConfig(String clusterId) {
        return memoryFallback.getFullConfig(clusterId);
    }

    @Override
    public void upsertNodeMetrics(String clusterId, EmbeddedAdminStateStore.NodeMetrics nodeMetrics) {
        memoryFallback.upsertNodeMetrics(clusterId, nodeMetrics);
        if (nodeMetrics == null || nodeMetrics.nodeId() == null || nodeMetrics.nodeId().isBlank()) {
            return;
        }
        put(nodeMetricsKey(clusterId, nodeMetrics.nodeId()), encodeNodeMetrics(nodeMetrics), "upsertNodeMetrics");
    }

    @Override
    public List<EmbeddedAdminStateStore.NodeMetrics> listNodeMetrics(String clusterId) {
        return memoryFallback.listNodeMetrics(clusterId);
    }

    @Override
    public void upsertClientSnapshot(String clusterId, EmbeddedAdminStateStore.ClientSnapshot clientSnapshot) {
        memoryFallback.upsertClientSnapshot(clusterId, clientSnapshot);
    }

    @Override
    public void removeClientSnapshot(String clusterId, String clientId) {
        memoryFallback.removeClientSnapshot(clusterId, clientId);
    }

    @Override
    public void replaceClientSubscriptions(String clusterId, String clientId, List<String> topics) {
        memoryFallback.replaceClientSubscriptions(clusterId, clientId, topics);
    }

    @Override
    public List<EmbeddedAdminStateStore.ClientSnapshot> listClientSnapshots(String clusterId) {
        return memoryFallback.listClientSnapshots(clusterId);
    }

    @Override
    public EmbeddedAdminStateStore.ClientSnapshot getClientSnapshot(String clusterId, String clientId) {
        return memoryFallback.getClientSnapshot(clusterId, clientId);
    }

    @Override
    public void upsertBlacklistEntry(String clusterId, EmbeddedAdminStateStore.BlacklistEntry entry) {
        memoryFallback.upsertBlacklistEntry(clusterId, entry);
        if (entry == null || entry.value() == null || entry.value().isBlank()) {
            return;
        }
        put(blacklistKey(clusterId, entry.type(), entry.value()), encodeBlacklistEntry(entry), "upsertBlacklistEntry");
    }

    @Override
    public void removeBlacklistEntry(String clusterId, String type, String value) {
        memoryFallback.removeBlacklistEntry(clusterId, type, value);
        try {
            db.delete(blacklistKey(clusterId, type, value));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[ADMIN-STATE] removeBlacklistEntry failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public List<EmbeddedAdminStateStore.BlacklistEntry> listBlacklistEntries(String clusterId) {
        return memoryFallback.listBlacklistEntries(clusterId);
    }

    @Override
    public void upsertClientTraceTask(String clusterId, ClientTraceManager.ClientTraceTask task) {
        memoryFallback.upsertClientTraceTask(clusterId, task);
        if (task == null || task.id() == null || task.id().isBlank()) {
            return;
        }
        put(clientTraceKey(clusterId, task.id()), encodeClientTraceTask(task), "upsertClientTraceTask");
    }

    @Override
    public void removeClientTraceTask(String clusterId, String taskId) {
        memoryFallback.removeClientTraceTask(clusterId, taskId);
        try {
            db.delete(clientTraceKey(clusterId, taskId));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[ADMIN-STATE] removeClientTraceTask failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public List<ClientTraceManager.ClientTraceTask> listClientTraceTasks(String clusterId) {
        return memoryFallback.listClientTraceTasks(clusterId);
    }

    @Override
    public void appendAuditLog(String clusterId, EmbeddedAdminStateStore.AuditLogEntry entry) {
        memoryFallback.appendAuditLog(clusterId, entry);
        if (entry == null || entry.id() == null || entry.id().isBlank()) {
            return;
        }
        put(auditLogKey(clusterId, entry.timestamp(), entry.id()), encodeAuditLog(entry), "appendAuditLog");
    }

    @Override
    public List<EmbeddedAdminStateStore.AuditLogEntry> listAuditLogs(String clusterId, int limit) {
        return memoryFallback.listAuditLogs(clusterId, limit);
    }

    @Override
    public void close() {
        db.close();
        options.close();
    }

    private void replayPersistedState() {
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                if (startsWith(key, CLUSTER_SUMMARY_PREFIX)) {
                    EmbeddedAdminStateStore.ClusterSummary summary = decodeClusterSummary(value);
                    if (summary != null) {
                        memoryFallback.createCluster(summary.clusterId(), summary.displayName(), summary.seedCoreNode());
                    }
                    continue;
                }
                if (startsWith(key, CLUSTER_CONFIG_PREFIX)) {
                    String clusterId = suffix(key, CLUSTER_CONFIG_PREFIX);
                    EmbeddedAdminStateStore.ClusterConfig config = decodeClusterConfig(value);
                    if (config != null) {
                        memoryFallback.setClusterConfig(clusterId, config);
                    }
                    continue;
                }
                if (startsWith(key, SECURITY_CONFIG_PREFIX)) {
                    String clusterId = suffix(key, SECURITY_CONFIG_PREFIX);
                    EmbeddedAdminStateStore.SecurityConfig config = decodeSecurityConfig(value);
                    if (config != null) {
                        memoryFallback.setSecurityConfig(clusterId, config);
                    }
                    continue;
                }
                if (startsWith(key, BRIDGE_CONFIG_PREFIX)) {
                    String clusterId = suffix(key, BRIDGE_CONFIG_PREFIX);
                    EmbeddedAdminStateStore.BridgeConfig config = decodeBridgeConfig(value);
                    if (config != null) {
                        memoryFallback.setBridgeConfig(clusterId, config);
                    }
                    continue;
                }
                if (startsWith(key, NODE_METRICS_PREFIX)) {
                    String composite = suffix(key, NODE_METRICS_PREFIX);
                    int split = composite.indexOf(':');
                    if (split <= 0) {
                        continue;
                    }
                    String clusterId = composite.substring(0, split);
                    EmbeddedAdminStateStore.NodeMetrics metrics = decodeNodeMetrics(value);
                    if (metrics != null) {
                        memoryFallback.upsertNodeMetrics(clusterId, metrics);
                    }
                    continue;
                }
                if (startsWith(key, BLACKLIST_PREFIX)) {
                    String composite = suffix(key, BLACKLIST_PREFIX);
                    int first = composite.indexOf(':');
                    int second = composite.indexOf(':', first + 1);
                    if (first <= 0 || second <= first) {
                        continue;
                    }
                    String clusterId = composite.substring(0, first);
                    EmbeddedAdminStateStore.BlacklistEntry entry = decodeBlacklistEntry(value);
                    if (entry != null) {
                        memoryFallback.upsertBlacklistEntry(clusterId, entry);
                    }
                    continue;
                }
                if (startsWith(key, CLIENT_TRACE_PREFIX)) {
                    String composite = suffix(key, CLIENT_TRACE_PREFIX);
                    int split = composite.indexOf(':');
                    if (split <= 0) {
                        continue;
                    }
                    String clusterId = composite.substring(0, split);
                    ClientTraceManager.ClientTraceTask task = decodeClientTraceTask(value);
                    if (task != null) {
                        memoryFallback.upsertClientTraceTask(clusterId, task);
                    }
                    continue;
                }
                if (startsWith(key, AUDIT_LOG_PREFIX)) {
                    String composite = suffix(key, AUDIT_LOG_PREFIX);
                    int split = composite.indexOf(':');
                    if (split <= 0) {
                        continue;
                    }
                    String clusterId = composite.substring(0, split);
                    EmbeddedAdminStateStore.AuditLogEntry entry = decodeAuditLog(value);
                    if (entry != null) {
                        memoryFallback.appendAuditLog(clusterId, entry);
                    }
                    continue;
                }
                if (matches(key, ADMIN_AUTH_CONFIG_KEY)) {
                    AdminAuthRuntime.Config config = decodeAdminAuthConfig(value);
                    if (config != null) {
                        memoryFallback.setAdminAuthConfig(config);
                    }
                }
            }
        }
    }

    private static byte[] encodeAdminAuthConfig(AdminAuthRuntime.Config config) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeString(data, config.username());
            writeString(data, config.password());
            writeString(data, config.role());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode admin auth config failed", exception);
        }
    }

    private static AdminAuthRuntime.Config decodeAdminAuthConfig(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new AdminAuthRuntime.Config(
                    readString(in),
                    readString(in),
                    readString(in)
            ).normalize();
        } catch (Exception ignored) {
            return null;
        }
    }

    private void put(byte[] key, byte[] value, String operation) {
        try {
            db.put(key, value);
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[ADMIN-STATE] " + operation + " failed: " + exception.getMessage(), exception);
        }
    }

    private static byte[] encodeClusterSummary(EmbeddedAdminStateStore.ClusterSummary summary) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeString(data, summary.clusterId());
            writeString(data, summary.displayName());
            writeString(data, summary.seedCoreNode());
            data.writeLong(summary.createdAt());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode cluster summary failed", exception);
        }
    }

    private static byte[] encodeBlacklistEntry(EmbeddedAdminStateStore.BlacklistEntry entry) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeString(data, entry.type());
            writeString(data, entry.value());
            data.writeLong(entry.createdAt());
            writeString(data, entry.source());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode blacklist entry failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.BlacklistEntry decodeBlacklistEntry(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new EmbeddedAdminStateStore.BlacklistEntry(
                    readString(in),
                    readString(in),
                    in.readLong(),
                    readString(in)
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeClientTraceTask(ClientTraceManager.ClientTraceTask task) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeString(data, task.id());
            writeString(data, task.clientId());
            data.writeLong(task.startAt());
            data.writeLong(task.endAt());
            data.writeLong(task.createdAt());
            writeString(data, task.createdBy());
            writeString(data, task.filePath());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode client trace task failed", exception);
        }
    }

    private static ClientTraceManager.ClientTraceTask decodeClientTraceTask(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new ClientTraceManager.ClientTraceTask(
                    readString(in),
                    readString(in),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    readString(in),
                    readString(in)
            ).normalize();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static EmbeddedAdminStateStore.ClusterSummary decodeClusterSummary(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new EmbeddedAdminStateStore.ClusterSummary(
                    readString(in),
                    readString(in),
                    readString(in),
                    in.readLong()
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeClusterConfig(EmbeddedAdminStateStore.ClusterConfig config) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeStringList(data, config.coreNodes());
            writeStringList(data, config.replicantNodes());
            data.writeBoolean(config.coreAcceptClientConnections());
            data.writeInt(config.sharedSubscriptionMaxMembersPerGroup());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode cluster config failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.ClusterConfig decodeClusterConfig(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new EmbeddedAdminStateStore.ClusterConfig(
                    readStringList(in),
                    readStringList(in),
                    in.readBoolean(),
                    in.readInt()
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeBridgeConfig(EmbeddedAdminStateStore.BridgeConfig config) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(3);
            data.writeBoolean(config.enabled());
            writeStringList(data, config.types());
            writeStringList(data, config.topicFilters());
            data.writeBoolean(config.asyncEnabled());
            data.writeInt(config.asyncQueueCapacity());
            data.writeInt(config.asyncWorkerCount());
            data.writeBoolean(config.kafka().enabled());
            writeString(data, config.kafka().bootstrapServers());
            writeString(data, config.kafka().topic());
            writeStringList(data, config.kafka().sourceTopicFilters());
            writeString(data, config.kafka().acks());
            writeString(data, config.kafka().clientId());
            writeString(data, config.kafka().compressionType());
            data.writeBoolean(config.rocketmq().enabled());
            writeString(data, config.rocketmq().nameServer());
            writeString(data, config.rocketmq().producerGroup());
            writeString(data, config.rocketmq().topic());
            writeStringList(data, config.rocketmq().sourceTopicFilters());
            data.writeBoolean(config.rocketmq().syncSend());
            data.writeInt(config.rocketmq().timeoutMs());
            data.writeBoolean(config.mysql().enabled());
            writeString(data, config.mysql().driver());
            writeString(data, config.mysql().url());
            writeString(data, config.mysql().user());
            writeString(data, config.mysql().password());
            writeString(data, config.mysql().table());
            writeStringList(data, config.mysql().sourceTopicFilters());
            data.writeBoolean(config.mysql().autoCreateTable());
            data.writeInt(config.mysql().poolMinIdle());
            data.writeInt(config.mysql().poolMaxSize());
            data.writeLong(config.mysql().poolConnectionTimeoutMs());
            data.writeLong(config.mysql().poolIdleTimeoutMs());
            data.writeLong(config.mysql().poolMaxLifetimeMs());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode bridge config failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.BridgeConfig decodeBridgeConfig(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            int version = in.readByte();
            if (version == 3) {
                return new EmbeddedAdminStateStore.BridgeConfig(
                        in.readBoolean(),
                        readStringList(in),
                        readStringList(in),
                        in.readBoolean(),
                        in.readInt(),
                        in.readInt(),
                        new EmbeddedAdminStateStore.BridgeKafkaConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.BridgeRocketmqConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                in.readBoolean(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.BridgeMysqlConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                in.readBoolean(),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version == 2) {
                EmbeddedAdminStateStore.BridgeMysqlConfig defaults = EmbeddedAdminStateStore.BridgeMysqlConfig.defaults();
                return new EmbeddedAdminStateStore.BridgeConfig(
                        in.readBoolean(),
                        readStringList(in),
                        readStringList(in),
                        in.readBoolean(),
                        in.readInt(),
                        in.readInt(),
                        new EmbeddedAdminStateStore.BridgeKafkaConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.BridgeRocketmqConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                in.readBoolean(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.BridgeMysqlConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                in.readBoolean(),
                                defaults.poolMinIdle(),
                                defaults.poolMaxSize(),
                                defaults.poolConnectionTimeoutMs(),
                                defaults.poolIdleTimeoutMs(),
                                defaults.poolMaxLifetimeMs()
                        )
                );
            }
            if (version != 1) {
                return null;
            }
            EmbeddedAdminStateStore.BridgeMysqlConfig defaults = EmbeddedAdminStateStore.BridgeMysqlConfig.defaults();
            return new EmbeddedAdminStateStore.BridgeConfig(
                    in.readBoolean(),
                    readStringList(in),
                    readStringList(in),
                    in.readBoolean(),
                    in.readInt(),
                    in.readInt(),
                    new EmbeddedAdminStateStore.BridgeKafkaConfig(
                            false,
                            readString(in),
                            readString(in),
                            readStringList(in),
                            readString(in),
                            readString(in),
                            readString(in)
                    ),
                    new EmbeddedAdminStateStore.BridgeRocketmqConfig(
                            false,
                            readString(in),
                            readString(in),
                            readString(in),
                            readStringList(in),
                            in.readBoolean(),
                            in.readInt()
                    ),
                    new EmbeddedAdminStateStore.BridgeMysqlConfig(
                            false,
                            readString(in),
                            readString(in),
                            readString(in),
                            readString(in),
                            readString(in),
                            readStringList(in),
                            in.readBoolean(),
                            defaults.poolMinIdle(),
                            defaults.poolMaxSize(),
                            defaults.poolConnectionTimeoutMs(),
                            defaults.poolIdleTimeoutMs(),
                            defaults.poolMaxLifetimeMs()
                    )
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeSecurityConfig(EmbeddedAdminStateStore.SecurityConfig config) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(11);
            data.writeBoolean(config.aclEnabled());
            writeStringList(data, config.aclChain());
            data.writeBoolean(config.aclDefaultAllow());
            writeString(data, config.aclHttp().url());
            data.writeInt(config.aclHttp().timeoutMs());
            writeString(data, config.aclHttp().bodyTemplate());
            writeString(data, config.aclFile().path());
            writeString(data, config.aclRedis().host());
            data.writeInt(config.aclRedis().port());
            writeString(data, config.aclRedis().password());
            data.writeInt(config.aclRedis().db());
            writeString(data, config.aclRedis().keyPrefix());
            data.writeInt(config.aclRedis().timeoutMs());
            data.writeBoolean(config.authEnabled());
            writeStringList(data, config.authChain());
            data.writeLong(config.cacheTtlMs());
            writeString(data, config.authHttp().method());
            writeString(data, config.authHttp().url());
            writeString(data, config.authHttp().headersText());
            data.writeBoolean(config.authHttp().tlsEnabled());
            writeString(data, config.authHttp().bodyTemplate());
            data.writeInt(config.authHttp().poolSize());
            data.writeInt(config.authHttp().rateLimitPerSecond());
            data.writeInt(config.authHttp().requestTimeoutMs());
            data.writeInt(config.authHttp().connectTimeoutMs());
            data.writeInt(config.authHttp().pipelineCount());
            writeString(data, config.authFile().path());
            writeString(data, config.authBuiltInDatabase().accountType());
            writeString(data, config.authBuiltInDatabase().passwordHashAlgorithm());
            writeString(data, config.authBuiltInDatabase().saltPosition());
            writeString(data, config.authRedis().host());
            data.writeInt(config.authRedis().port());
            writeString(data, config.authRedis().password());
            data.writeInt(config.authRedis().db());
            writeString(data, config.authRedis().keyPrefix());
            data.writeInt(config.authRedis().timeoutMs());
            writeString(data, config.authMysql().url());
            writeString(data, config.authMysql().user());
            writeString(data, config.authMysql().password());
            writeString(data, config.authMysql().query());
            data.writeInt(config.authMysql().poolMinIdle());
            data.writeInt(config.authMysql().poolMaxSize());
            data.writeLong(config.authMysql().poolConnectionTimeoutMs());
            data.writeLong(config.authMysql().poolIdleTimeoutMs());
            data.writeLong(config.authMysql().poolMaxLifetimeMs());
            writeString(data, config.authPostgresql().url());
            writeString(data, config.authPostgresql().user());
            writeString(data, config.authPostgresql().password());
            writeString(data, config.authPostgresql().query());
            data.writeInt(config.authPostgresql().poolMinIdle());
            data.writeInt(config.authPostgresql().poolMaxSize());
            data.writeLong(config.authPostgresql().poolConnectionTimeoutMs());
            data.writeLong(config.authPostgresql().poolIdleTimeoutMs());
            data.writeLong(config.authPostgresql().poolMaxLifetimeMs());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode security config failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.SecurityConfig decodeSecurityConfig(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            int version = in.readByte();
            if (version == 11) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        new EmbeddedAdminStateStore.AclHttpConfig(
                                readString(in),
                                in.readInt(),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AclFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AclRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        new EmbeddedAdminStateStore.AuthHttpConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readBoolean(),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        ),
                        new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version == 10) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        new EmbeddedAdminStateStore.AclHttpConfig(
                                readString(in),
                                in.readInt(),
                                EmbeddedAdminStateStore.AclHttpConfig.defaults().bodyTemplate()
                        ),
                        new EmbeddedAdminStateStore.AclFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AclRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        new EmbeddedAdminStateStore.AuthHttpConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readBoolean(),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        ),
                        new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version == 1) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong()
                );
            }
            if (version == 2) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        EmbeddedAdminStateStore.AuthHttpConfig.defaults(),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig.defaults(),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                1,
                                8,
                                3000,
                                60_000,
                                600_000
                        ),
                        EmbeddedAdminStateStore.AuthPostgresqlConfig.defaults()
                );
            }
            if (version == 3) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        EmbeddedAdminStateStore.AuthHttpConfig.defaults(),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig.defaults(),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                1,
                                8,
                                3000,
                                60_000,
                                600_000
                        ),
                        EmbeddedAdminStateStore.AuthPostgresqlConfig.defaults()
                );
            }
            if (version == 4) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        EmbeddedAdminStateStore.AuthHttpConfig.defaults(),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig("username", "plain", "disable"),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                1,
                                8,
                                3000,
                                60_000,
                                600_000
                        ),
                        EmbeddedAdminStateStore.AuthPostgresqlConfig.defaults()
                );
            }
            if (version == 5) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        EmbeddedAdminStateStore.AuthHttpConfig.defaults(),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                1,
                                8,
                                3000,
                                60_000,
                                600_000
                        ),
                        EmbeddedAdminStateStore.AuthPostgresqlConfig.defaults()
                );
            }
            if (version == 6) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        EmbeddedAdminStateStore.AuthHttpConfig.defaults(),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                1,
                                8,
                                3000,
                                60_000,
                                600_000
                        ),
                        EmbeddedAdminStateStore.AuthPostgresqlConfig.defaults()
                );
            }
            if (version == 7) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        new EmbeddedAdminStateStore.AuthHttpConfig(
                                "POST",
                                readString(in),
                                "content-type: application/json",
                                false,
                                "{\n  \"username\": \"${username}\",\n  \"password\": \"${password}\"\n}",
                                4,
                                0,
                                in.readInt(),
                                1500,
                                2
                        ),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        ),
                        new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version == 8) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        new EmbeddedAdminStateStore.AuthHttpConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readBoolean(),
                                readString(in),
                                in.readInt(),
                                0,
                                in.readInt(),
                                in.readInt(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        ),
                        new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version != 9) {
                return null;
            }
            return new EmbeddedAdminStateStore.SecurityConfig(
                    in.readBoolean(),
                    readStringList(in),
                    in.readBoolean(),
                    readStringList(in),
                    in.readLong(),
                    new EmbeddedAdminStateStore.AuthHttpConfig(
                            readString(in),
                            readString(in),
                            readString(in),
                            in.readBoolean(),
                            readString(in),
                            in.readInt(),
                            in.readInt(),
                            in.readInt(),
                            in.readInt(),
                            in.readInt()
                    ),
                    new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                    new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                            readString(in),
                            readString(in),
                            readString(in)
                    ),
                    new EmbeddedAdminStateStore.AuthRedisConfig(
                            readString(in),
                            in.readInt(),
                            readString(in),
                            in.readInt(),
                            readString(in),
                            in.readInt()
                    ),
                    new EmbeddedAdminStateStore.AuthMysqlConfig(
                            readString(in),
                            readString(in),
                            readString(in),
                            readString(in),
                            in.readInt(),
                            in.readInt(),
                            in.readLong(),
                            in.readLong(),
                            in.readLong()
                    ),
                    new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                            readString(in),
                            readString(in),
                            readString(in),
                            readString(in),
                            in.readInt(),
                            in.readInt(),
                            in.readLong(),
                            in.readLong(),
                            in.readLong()
                    )
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeNodeMetrics(EmbeddedAdminStateStore.NodeMetrics metrics) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(2);
            writeString(data, metrics.nodeId());
            writeString(data, metrics.nodeIp());
            writeString(data, metrics.role());
            data.writeLong(metrics.inboundBytes());
            data.writeLong(metrics.outboundBytes());
            data.writeInt(metrics.connectedClients());
            data.writeLong(metrics.reportTime());
            data.writeLong(metrics.connectAuthSuccess());
            data.writeLong(metrics.connectAuthFailure());
            data.writeLong(metrics.connectAuthError());
            data.writeLong(metrics.connectAuthSlow());
            data.writeLong(metrics.connectAuthAvgMs());
            data.writeLong(metrics.connectAuthMaxMs());
            data.writeLong(metrics.publishAclAllow());
            data.writeLong(metrics.publishAclDeny());
            data.writeLong(metrics.publishAclError());
            data.writeLong(metrics.publishAclSlow());
            data.writeLong(metrics.publishAclAvgMs());
            data.writeLong(metrics.publishAclMaxMs());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode node metrics failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.NodeMetrics decodeNodeMetrics(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            int version = in.readByte();
            if (version != 1 && version != 2) {
                return null;
            }
            String nodeId = readString(in);
            String nodeIp = readString(in);
            String role = readString(in);
            long inboundBytes = in.readLong();
            long outboundBytes = in.readLong();
            int connectedClients = in.readInt();
            long reportTime = in.readLong();
            if (version == 1) {
                return new EmbeddedAdminStateStore.NodeMetrics(
                        nodeId,
                        nodeIp,
                        role,
                        inboundBytes,
                        outboundBytes,
                        connectedClients,
                        reportTime,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L
                );
            }
            return new EmbeddedAdminStateStore.NodeMetrics(
                    nodeId,
                    nodeIp,
                    role,
                    inboundBytes,
                    outboundBytes,
                    connectedClients,
                    reportTime,
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong()
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeAuditLog(EmbeddedAdminStateStore.AuditLogEntry entry) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeString(data, entry.id());
            writeString(data, entry.clusterId());
            writeString(data, entry.action());
            writeString(data, entry.source());
            data.writeLong(entry.timestamp());
            writeString(data, entry.beforeJson());
            writeString(data, entry.afterJson());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode audit log failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.AuditLogEntry decodeAuditLog(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new EmbeddedAdminStateStore.AuditLogEntry(
                    readString(in),
                    readString(in),
                    readString(in),
                    readString(in),
                    in.readLong(),
                    readString(in),
                    readString(in)
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void writeString(DataOutputStream out, String value) throws Exception {
        byte[] bytes = normalize(value).getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws Exception {
        int length = in.readInt();
        if (length < 0) {
            return "";
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            return "";
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeStringList(DataOutputStream out, List<String> values) throws Exception {
        List<String> safeValues = values == null ? List.of() : values;
        out.writeInt(safeValues.size());
        for (String value : safeValues) {
            writeString(out, value);
        }
    }

    private static List<String> readStringList(DataInputStream in) throws Exception {
        int size = in.readInt();
        if (size <= 0) {
            return List.of();
        }
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(readString(in));
        }
        return values;
    }

    private static byte[] key(byte[] prefix, String suffix) {
        byte[] suffixBytes = normalize(suffix).getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefix.length + suffixBytes.length];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(suffixBytes, 0, key, prefix.length, suffixBytes.length);
        return key;
    }

    private static byte[] nodeMetricsKey(String clusterId, String nodeId) {
        return key(NODE_METRICS_PREFIX, normalize(clusterId) + ":" + normalize(nodeId));
    }

    private static byte[] blacklistKey(String clusterId, String type, String value) {
        return key(BLACKLIST_PREFIX, normalize(clusterId) + ":" + normalize(type) + ":" + normalize(value));
    }

    private static byte[] clientTraceKey(String clusterId, String taskId) {
        return key(CLIENT_TRACE_PREFIX, normalize(clusterId) + ":" + normalize(taskId));
    }

    private static byte[] auditLogKey(String clusterId, long timestamp, String id) {
        return key(AUDIT_LOG_PREFIX, normalize(clusterId) + ":" + String.format("%019d", timestamp) + ":" + normalize(id));
    }

    private static boolean startsWith(byte[] source, byte[] prefix) {
        if (source == null || prefix == null || source.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (source[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean matches(byte[] source, byte[] target) {
        if (source == null || target == null || source.length != target.length) {
            return false;
        }
        for (int i = 0; i < source.length; i++) {
            if (source[i] != target[i]) {
                return false;
            }
        }
        return true;
    }

    private static String suffix(byte[] source, byte[] prefix) {
        return new String(source, prefix.length, source.length - prefix.length, StandardCharsets.UTF_8);
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.trim();
    }

    private static String normalizePath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return "data/admin-state-rocksdb";
        }
        return rawPath.trim();
    }

    private static void ensureParentDirectory(String dbPath) {
        Path path = Path.of(dbPath).toAbsolutePath();
        Path parent = path.getParent();
        if (parent == null) {
            return;
        }
        try {
            Files.createDirectories(parent);
        } catch (Exception exception) {
            throw new IllegalStateException("create admin-state rocksdb parent dir failed: " + parent, exception);
        }
    }
}
