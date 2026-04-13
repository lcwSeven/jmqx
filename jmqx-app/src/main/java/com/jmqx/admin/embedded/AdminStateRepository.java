package com.jmqx.admin.embedded;

import java.util.List;

/**
 * 管理端状态仓储。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public interface AdminStateRepository extends AutoCloseable {

    EmbeddedAdminStateStore.ClusterSummary createCluster(String clusterId, String displayName, String seedCoreNode);

    List<EmbeddedAdminStateStore.ClusterSummary> listClusters();

    EmbeddedAdminStateStore.ClusterSummary getClusterSummary(String clusterId);

    EmbeddedAdminStateStore.ClusterConfig getClusterConfig(String clusterId);

    void setClusterConfig(String clusterId, EmbeddedAdminStateStore.ClusterConfig clusterConfig);

    EmbeddedAdminStateStore.SecurityConfig getSecurityConfig(String clusterId);

    void setSecurityConfig(String clusterId, EmbeddedAdminStateStore.SecurityConfig securityConfig);

    EmbeddedAdminStateStore.ClusterFullConfig getFullConfig(String clusterId);

    void upsertNodeMetrics(String clusterId, EmbeddedAdminStateStore.NodeMetrics nodeMetrics);

    List<EmbeddedAdminStateStore.NodeMetrics> listNodeMetrics(String clusterId);

    void appendAuditLog(String clusterId, EmbeddedAdminStateStore.AuditLogEntry entry);

    List<EmbeddedAdminStateStore.AuditLogEntry> listAuditLogs(String clusterId, int limit);

    @Override
    default void close() {
    }
}
