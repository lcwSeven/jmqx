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

    EmbeddedAdminStateStore.ClusterFullConfig getFullConfig(String clusterId);

    void upsertNodeMetrics(String clusterId, EmbeddedAdminStateStore.NodeMetrics nodeMetrics);

    List<EmbeddedAdminStateStore.NodeMetrics> listNodeMetrics(String clusterId);

    void appendAuditLog(String clusterId, EmbeddedAdminStateStore.AuditLogEntry entry);

    List<EmbeddedAdminStateStore.AuditLogEntry> listAuditLogs(String clusterId, int limit);

    @Override
    default void close() {
    }
}
