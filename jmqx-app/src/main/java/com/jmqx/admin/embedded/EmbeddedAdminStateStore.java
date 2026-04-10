package com.jmqx.admin.embedded;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内嵌管理端状态存储。
 *
 * @author liucaiwen
 * @since 2026-04-10
 */
public class EmbeddedAdminStateStore {

    private final Map<String, ClusterState> clusterStates = new ConcurrentHashMap<>();

    public EmbeddedAdminStateStore() {
        createCluster("default", "默认集群", "127.0.0.1:7800");
    }

    public ClusterSummary createCluster(String clusterId, String displayName, String seedCoreNode) {
        String id = normalize(clusterId, "default");
        ClusterState state = clusterStates.computeIfAbsent(id, ignored -> new ClusterState(
                new ClusterSummary(id, normalize(displayName, id), normalize(seedCoreNode, "unknown"), System.currentTimeMillis()),
                new ClusterConfig(List.of(normalize(seedCoreNode, "unknown")), List.of(), true, 10_000),
                new SecurityConfig(true, List.of("file"), true, List.of("file"), 60_000)
        ));
        return state.summary;
    }

    public List<ClusterSummary> listClusters() {
        List<ClusterSummary> clusters = new ArrayList<>();
        for (ClusterState state : clusterStates.values()) {
            clusters.add(state.summary);
        }
        clusters.sort(Comparator.comparing(ClusterSummary::clusterId));
        return clusters;
    }

    public ClusterSummary getClusterSummary(String clusterId) {
        return getOrCreate(clusterId).summary;
    }

    public ClusterConfig getClusterConfig(String clusterId) {
        return getOrCreate(clusterId).clusterConfig;
    }

    public void setClusterConfig(String clusterId, ClusterConfig clusterConfig) {
        getOrCreate(clusterId).clusterConfig = clusterConfig;
    }

    public SecurityConfig getSecurityConfig(String clusterId) {
        return getOrCreate(clusterId).securityConfig;
    }

    public void setSecurityConfig(String clusterId, SecurityConfig securityConfig) {
        getOrCreate(clusterId).securityConfig = securityConfig;
    }

    public ClusterFullConfig getFullConfig(String clusterId) {
        ClusterState state = getOrCreate(clusterId);
        return new ClusterFullConfig(state.summary, state.clusterConfig, state.securityConfig);
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

    private static final class ClusterState {
        private final ClusterSummary summary;
        private volatile ClusterConfig clusterConfig;
        private volatile SecurityConfig securityConfig;

        private ClusterState(ClusterSummary summary, ClusterConfig clusterConfig, SecurityConfig securityConfig) {
            this.summary = summary;
            this.clusterConfig = clusterConfig;
            this.securityConfig = securityConfig;
        }
    }
}
