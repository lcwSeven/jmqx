package com.jmqx.admin.embedded;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 内嵌管理端状态存储。
 *
 * @author liucaiwen
 * @since 2026-04-10
 */
public class EmbeddedAdminStateStore implements AdminStateRepository {

    private final Map<String, ClusterState> clusterStates = new ConcurrentHashMap<>();

    public EmbeddedAdminStateStore() {
        createCluster("default", "默认集群", "127.0.0.1:7800");
    }

    @Override
    public ClusterSummary createCluster(String clusterId, String displayName, String seedCoreNode) {
        String id = normalize(clusterId, "default");
        ClusterState state = clusterStates.computeIfAbsent(id, ignored -> new ClusterState(
                new ClusterSummary(id, normalize(displayName, id), normalize(seedCoreNode, "unknown"), System.currentTimeMillis()),
                new ClusterConfig(List.of(normalize(seedCoreNode, "unknown")), List.of(), true, 10_000),
                new SecurityConfig(true, List.of("file"), true, List.of("file"), 60_000)
        ));
        return state.summary;
    }

    @Override
    public List<ClusterSummary> listClusters() {
        List<ClusterSummary> clusters = new ArrayList<>();
        for (ClusterState state : clusterStates.values()) {
            clusters.add(state.summary);
        }
        clusters.sort(Comparator.comparing(ClusterSummary::clusterId));
        return clusters;
    }

    @Override
    public ClusterSummary getClusterSummary(String clusterId) {
        return getOrCreate(clusterId).summary;
    }

    @Override
    public ClusterConfig getClusterConfig(String clusterId) {
        return getOrCreate(clusterId).clusterConfig;
    }

    @Override
    public void setClusterConfig(String clusterId, ClusterConfig clusterConfig) {
        getOrCreate(clusterId).clusterConfig = clusterConfig;
    }

    @Override
    public SecurityConfig getSecurityConfig(String clusterId) {
        return getOrCreate(clusterId).securityConfig;
    }

    @Override
    public void setSecurityConfig(String clusterId, SecurityConfig securityConfig) {
        getOrCreate(clusterId).securityConfig = securityConfig;
    }

    @Override
    public ClusterFullConfig getFullConfig(String clusterId) {
        ClusterState state = getOrCreate(clusterId);
        return new ClusterFullConfig(state.summary, state.clusterConfig, state.securityConfig);
    }

    @Override
    public void upsertNodeMetrics(String clusterId, NodeMetrics nodeMetrics) {
        if (nodeMetrics == null || nodeMetrics.nodeId() == null || nodeMetrics.nodeId().isBlank()) {
            return;
        }
        getOrCreate(clusterId).nodeMetrics.put(nodeMetrics.nodeId(), nodeMetrics);
    }

    @Override
    public List<NodeMetrics> listNodeMetrics(String clusterId) {
        List<NodeMetrics> metrics = new ArrayList<>(getOrCreate(clusterId).nodeMetrics.values());
        metrics.sort(Comparator.comparing(NodeMetrics::nodeId));
        return metrics;
    }

    @Override
    public void appendAuditLog(String clusterId, AuditLogEntry entry) {
        if (entry == null) {
            return;
        }
        ClusterState state = getOrCreate(clusterId);
        state.auditLogs.add(0, entry);
        while (state.auditLogs.size() > 500) {
            state.auditLogs.remove(state.auditLogs.size() - 1);
        }
    }

    @Override
    public List<AuditLogEntry> listAuditLogs(String clusterId, int limit) {
        List<AuditLogEntry> logs = new ArrayList<>(getOrCreate(clusterId).auditLogs);
        int safeLimit = Math.max(1, limit);
        return logs.size() <= safeLimit ? logs : logs.subList(0, safeLimit);
    }

    private ClusterState getOrCreate(String clusterId) {
        String id = normalize(clusterId, "default");
        return clusterStates.computeIfAbsent(id, ignored -> new ClusterState(
                new ClusterSummary(id, "集群-" + id, "unknown", System.currentTimeMillis()),
                new ClusterConfig(List.of(), List.of(), true, 10_000),
                new SecurityConfig(true, List.of("file"), true, List.of("file"), 60_000)
        ));
    }

    private static String normalize(String value, String defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    /**
     * 集群摘要。
     */
    public record ClusterSummary(String clusterId, String displayName, String seedCoreNode, long createdAt) {
    }

    /**
     * 集群配置。
     */
    public record ClusterConfig(
            List<String> coreNodes,
            List<String> replicantNodes,
            boolean coreAcceptClientConnections,
            int sharedSubscriptionMaxMembersPerGroup
    ) {
    }

    /**
     * 安全配置。
     */
    public record SecurityConfig(
            boolean aclEnabled,
            List<String> aclChain,
            boolean authEnabled,
            List<String> authChain,
            long cacheTtlMs
    ) {
    }

    /**
     * 完整配置。
     */
    public record ClusterFullConfig(ClusterSummary summary, ClusterConfig clusterConfig, SecurityConfig securityConfig) {
    }

    /**
     * 节点指标快照。
     */
    public record NodeMetrics(
            String nodeId,
            String nodeIp,
            String role,
            long inboundBytes,
            long outboundBytes,
            int connectedClients,
            long reportTime
    ) {
    }

    /**
     * 配置变更审计日志。
     */
    public record AuditLogEntry(
            String id,
            String clusterId,
            String action,
            String source,
            long timestamp,
            String beforeJson,
            String afterJson
    ) {
    }

    private static final class ClusterState {
        private final ClusterSummary summary;
        private volatile ClusterConfig clusterConfig;
        private volatile SecurityConfig securityConfig;
        private final Map<String, NodeMetrics> nodeMetrics = new ConcurrentHashMap<>();
        private final List<AuditLogEntry> auditLogs = new CopyOnWriteArrayList<>();

        private ClusterState(ClusterSummary summary, ClusterConfig clusterConfig, SecurityConfig securityConfig) {
            this.summary = summary;
            this.clusterConfig = clusterConfig;
            this.securityConfig = securityConfig;
        }
    }
}
