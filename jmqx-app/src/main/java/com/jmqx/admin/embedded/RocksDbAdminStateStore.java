package com.jmqx.admin.embedded;

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
    private static final byte[] NODE_METRICS_PREFIX = "node-metrics:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AUDIT_LOG_PREFIX = "audit-log:".getBytes(StandardCharsets.UTF_8);

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
    public EmbeddedAdminStateStore.SecurityConfig getSecurityConfig(String clusterId) {
        return memoryFallback.getSecurityConfig(clusterId);
    }

    @Override
    public void setSecurityConfig(String clusterId, EmbeddedAdminStateStore.SecurityConfig securityConfig) {
        memoryFallback.setSecurityConfig(clusterId, securityConfig);
        put(key(SECURITY_CONFIG_PREFIX, normalize(clusterId)), encodeSecurityConfig(securityConfig), "setSecurityConfig");
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
                }
            }
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

    private static byte[] encodeSecurityConfig(EmbeddedAdminStateStore.SecurityConfig config) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            data.writeBoolean(config.aclEnabled());
            writeStringList(data, config.aclChain());
            data.writeBoolean(config.authEnabled());
            writeStringList(data, config.authChain());
            data.writeLong(config.cacheTtlMs());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode security config failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.SecurityConfig decodeSecurityConfig(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new EmbeddedAdminStateStore.SecurityConfig(
                    in.readBoolean(),
                    readStringList(in),
                    in.readBoolean(),
                    readStringList(in),
                    in.readLong()
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeNodeMetrics(EmbeddedAdminStateStore.NodeMetrics metrics) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeString(data, metrics.nodeId());
            writeString(data, metrics.nodeIp());
            writeString(data, metrics.role());
            data.writeLong(metrics.inboundBytes());
            data.writeLong(metrics.outboundBytes());
            data.writeInt(metrics.connectedClients());
            data.writeLong(metrics.reportTime());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode node metrics failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.NodeMetrics decodeNodeMetrics(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new EmbeddedAdminStateStore.NodeMetrics(
                    readString(in),
                    readString(in),
                    readString(in),
                    in.readLong(),
                    in.readLong(),
                    in.readInt(),
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
