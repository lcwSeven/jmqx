package com.jmqx.admin.embedded;

import java.util.List;

/**
 * 管理端状态仓储。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public interface AdminStateRepository extends AutoCloseable {

    AdminAuthRuntime.Config getAdminAuthConfig();

    void setAdminAuthConfig(AdminAuthRuntime.Config adminAuthConfig);

    boolean hasAdminAuthConfig();

    EmbeddedAdminStateStore.ClusterSummary createCluster(String clusterId, String displayName, String seedCoreNode);

    List<EmbeddedAdminStateStore.ClusterSummary> listClusters();

    EmbeddedAdminStateStore.ClusterSummary getClusterSummary(String clusterId);

    EmbeddedAdminStateStore.ClusterConfig getClusterConfig(String clusterId);

    void setClusterConfig(String clusterId, EmbeddedAdminStateStore.ClusterConfig clusterConfig);

    boolean hasClusterConfig(String clusterId);

    EmbeddedAdminStateStore.SecurityConfig getSecurityConfig(String clusterId);

    void setSecurityConfig(String clusterId, EmbeddedAdminStateStore.SecurityConfig securityConfig);

    boolean hasSecurityConfig(String clusterId);

    EmbeddedAdminStateStore.BridgeConfig getBridgeConfig(String clusterId);

    void setBridgeConfig(String clusterId, EmbeddedAdminStateStore.BridgeConfig bridgeConfig);

    boolean hasBridgeConfig(String clusterId);

    EmbeddedAdminStateStore.ClusterFullConfig getFullConfig(String clusterId);

    void upsertNodeMetrics(String clusterId, EmbeddedAdminStateStore.NodeMetrics nodeMetrics);

    List<EmbeddedAdminStateStore.NodeMetrics> listNodeMetrics(String clusterId);

    void upsertClientSnapshot(String clusterId, EmbeddedAdminStateStore.ClientSnapshot clientSnapshot);

    void removeClientSnapshot(String clusterId, String clientId);

    void replaceClientSubscriptions(String clusterId, String clientId, List<String> topics);

    List<EmbeddedAdminStateStore.ClientSnapshot> listClientSnapshots(String clusterId);

    EmbeddedAdminStateStore.ClientSnapshot getClientSnapshot(String clusterId, String clientId);

    void upsertBlacklistEntry(String clusterId, EmbeddedAdminStateStore.BlacklistEntry entry);

    void removeBlacklistEntry(String clusterId, String type, String value);

    List<EmbeddedAdminStateStore.BlacklistEntry> listBlacklistEntries(String clusterId);

    void appendAuditLog(String clusterId, EmbeddedAdminStateStore.AuditLogEntry entry);

    List<EmbeddedAdminStateStore.AuditLogEntry> listAuditLogs(String clusterId, int limit);

    @Override
    default void close() {
    }
}
