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
    private volatile AdminAuthRuntime.Config adminAuthConfig = AdminAuthRuntime.Config.defaults();
    private volatile boolean adminAuthConfigInitialized;

    public EmbeddedAdminStateStore() {
        createCluster("default", "默认集群", "127.0.0.1:7800");
    }

    @Override
    public AdminAuthRuntime.Config getAdminAuthConfig() {
        return adminAuthConfig;
    }

    @Override
    public void setAdminAuthConfig(AdminAuthRuntime.Config adminAuthConfig) {
        this.adminAuthConfig = adminAuthConfig == null ? AdminAuthRuntime.Config.defaults() : adminAuthConfig.normalize();
        this.adminAuthConfigInitialized = true;
    }

    @Override
    public boolean hasAdminAuthConfig() {
        return adminAuthConfigInitialized;
    }

    @Override
    public ClusterSummary createCluster(String clusterId, String displayName, String seedCoreNode) {
        String id = normalize(clusterId, "default");
        ClusterState state = clusterStates.computeIfAbsent(id, ignored -> new ClusterState(
                new ClusterSummary(id, normalize(displayName, id), normalize(seedCoreNode, "unknown"), System.currentTimeMillis()),
                new ClusterConfig(List.of(normalize(seedCoreNode, "unknown")), List.of(), true, 10_000),
                SecurityConfig.defaultConfig()
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
        ClusterState state = getOrCreate(clusterId);
        state.clusterConfig = clusterConfig;
        state.clusterConfigInitialized = true;
    }

    @Override
    public boolean hasClusterConfig(String clusterId) {
        return getOrCreate(clusterId).clusterConfigInitialized;
    }

    @Override
    public SecurityConfig getSecurityConfig(String clusterId) {
        return getOrCreate(clusterId).securityConfig;
    }

    @Override
    public void setSecurityConfig(String clusterId, SecurityConfig securityConfig) {
        ClusterState state = getOrCreate(clusterId);
        state.securityConfig = securityConfig;
        state.securityConfigInitialized = true;
    }

    @Override
    public boolean hasSecurityConfig(String clusterId) {
        return getOrCreate(clusterId).securityConfigInitialized;
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
                SecurityConfig.defaultConfig()
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
            long cacheTtlMs,
            AuthHttpConfig authHttp,
            AuthFileConfig authFile,
            AuthBuiltInDatabaseConfig authBuiltInDatabase,
            AuthRedisConfig authRedis,
            AuthMysqlConfig authMysql,
            AuthPostgresqlConfig authPostgresql
    ) {
        public SecurityConfig(boolean aclEnabled,
                              List<String> aclChain,
                              boolean authEnabled,
                              List<String> authChain,
                              long cacheTtlMs) {
            this(
                    aclEnabled,
                    aclChain,
                    authEnabled,
                    authChain,
                    cacheTtlMs,
                    AuthHttpConfig.defaults(),
                    AuthFileConfig.defaults(),
                    AuthBuiltInDatabaseConfig.defaults(),
                    AuthRedisConfig.defaults(),
                    AuthMysqlConfig.defaults(),
                    AuthPostgresqlConfig.defaults()
            );
        }

        public SecurityConfig {
            aclChain = normalizeList(aclChain);
            authChain = normalizeList(authChain);
            authHttp = authHttp == null ? AuthHttpConfig.defaults() : authHttp;
            authFile = authFile == null ? AuthFileConfig.defaults() : authFile;
            authBuiltInDatabase = authBuiltInDatabase == null ? AuthBuiltInDatabaseConfig.defaults() : authBuiltInDatabase;
            authRedis = authRedis == null ? AuthRedisConfig.defaults() : authRedis;
            authMysql = authMysql == null ? AuthMysqlConfig.defaults() : authMysql;
            authPostgresql = authPostgresql == null ? AuthPostgresqlConfig.defaults() : authPostgresql;
        }

        public static SecurityConfig defaultConfig() {
            return new SecurityConfig(
                    true,
                    List.of("file"),
                    false,
                    List.of(),
                    60_000,
                    AuthHttpConfig.defaults(),
                    AuthFileConfig.defaults(),
                    AuthBuiltInDatabaseConfig.defaults(),
                    AuthRedisConfig.defaults(),
                    AuthMysqlConfig.defaults(),
                    AuthPostgresqlConfig.defaults()
            );
        }
    }

    public record AuthHttpConfig(
            String method,
            String url,
            String headersText,
            boolean tlsEnabled,
            String bodyTemplate,
            int poolSize,
            int rateLimitPerSecond,
            int requestTimeoutMs,
            int connectTimeoutMs,
            int pipelineCount
    ) {
        public AuthHttpConfig {
            method = normalize(method, "POST");
            url = normalize(url, "");
            headersText = headersText == null ? "" : headersText.trim();
            bodyTemplate = bodyTemplate == null ? "" : bodyTemplate;
        }

        public static AuthHttpConfig defaults() {
            return new AuthHttpConfig(
                    "POST",
                    "http://127.0.0.1:8080/auth/check",
                    "content-type: application/json",
                    false,
                    "{\n  \"username\": \"${username}\",\n  \"password\": \"${password}\"\n}",
                    4,
                    0,
                    2000,
                    1500,
                    2
            );
        }
    }

    public record AuthFileConfig(String path) {
        public AuthFileConfig {
            path = normalize(path, "auth-users.txt");
        }

        public static AuthFileConfig defaults() {
            return new AuthFileConfig("auth-users.txt");
        }
    }

    public record AuthBuiltInDatabaseConfig(
            String accountType,
            String passwordHashAlgorithm,
            String saltPosition
    ) {
        public AuthBuiltInDatabaseConfig {
            accountType = normalize(accountType, "username");
            passwordHashAlgorithm = normalize(passwordHashAlgorithm, "sha256");
            saltPosition = normalize(saltPosition, "suffix");
        }

        public static AuthBuiltInDatabaseConfig defaults() {
            return new AuthBuiltInDatabaseConfig(
                    "username",
                    "sha256",
                    "suffix"
            );
        }
    }

    public record AuthRedisConfig(String host, int port, String password, int db, String keyPrefix, int timeoutMs) {
        public AuthRedisConfig {
            host = normalize(host, "127.0.0.1");
            password = password == null ? "" : password;
            keyPrefix = normalize(keyPrefix, "jmqx:auth");
        }

        public static AuthRedisConfig defaults() {
            return new AuthRedisConfig("127.0.0.1", 6379, "", 0, "jmqx:auth", 2000);
        }
    }

    public record AuthMysqlConfig(
            String url,
            String user,
            String password,
            String query,
            int poolMinIdle,
            int poolMaxSize,
            long poolConnectionTimeoutMs,
            long poolIdleTimeoutMs,
            long poolMaxLifetimeMs
    ) {
        public AuthMysqlConfig {
            url = normalize(url, "jdbc:mysql://127.0.0.1:3306/jmqx");
            user = normalize(user, "root");
            password = password == null ? "" : password;
            query = normalize(query, "SELECT password FROM mqtt_user WHERE username = ?");
            poolMinIdle = Math.max(0, poolMinIdle);
            poolMaxSize = Math.max(1, poolMaxSize);
            poolConnectionTimeoutMs = Math.max(250L, poolConnectionTimeoutMs);
            poolIdleTimeoutMs = Math.max(10_000L, poolIdleTimeoutMs);
            poolMaxLifetimeMs = Math.max(30_000L, poolMaxLifetimeMs);
        }

        public static AuthMysqlConfig defaults() {
            return new AuthMysqlConfig(
                    "jdbc:mysql://127.0.0.1:3306/jmqx",
                    "root",
                    "",
                    "SELECT password FROM mqtt_user WHERE username = ?",
                    1,
                    8,
                    3000,
                    60_000,
                    600_000
            );
        }
    }

    public record AuthPostgresqlConfig(
            String url,
            String user,
            String password,
            String query,
            int poolMinIdle,
            int poolMaxSize,
            long poolConnectionTimeoutMs,
            long poolIdleTimeoutMs,
            long poolMaxLifetimeMs
    ) {
        public AuthPostgresqlConfig {
            url = normalize(url, "jdbc:postgresql://127.0.0.1:5432/jmqx");
            user = normalize(user, "postgres");
            password = password == null ? "" : password;
            query = normalize(query, "SELECT password FROM mqtt_user WHERE username = ?");
            poolMinIdle = Math.max(0, poolMinIdle);
            poolMaxSize = Math.max(1, poolMaxSize);
            poolConnectionTimeoutMs = Math.max(250L, poolConnectionTimeoutMs);
            poolIdleTimeoutMs = Math.max(10_000L, poolIdleTimeoutMs);
            poolMaxLifetimeMs = Math.max(30_000L, poolMaxLifetimeMs);
        }

        public static AuthPostgresqlConfig defaults() {
            return new AuthPostgresqlConfig(
                    "jdbc:postgresql://127.0.0.1:5432/jmqx",
                    "postgres",
                    "",
                    "SELECT password FROM mqtt_user WHERE username = ?",
                    1,
                    8,
                    3000,
                    60_000,
                    600_000
            );
        }
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
        private volatile boolean clusterConfigInitialized;
        private volatile boolean securityConfigInitialized;
        private final Map<String, NodeMetrics> nodeMetrics = new ConcurrentHashMap<>();
        private final List<AuditLogEntry> auditLogs = new CopyOnWriteArrayList<>();

        private ClusterState(ClusterSummary summary, ClusterConfig clusterConfig, SecurityConfig securityConfig) {
            this.summary = summary;
            this.clusterConfig = clusterConfig;
            this.securityConfig = securityConfig;
            this.clusterConfigInitialized = false;
            this.securityConfigInitialized = false;
        }
    }

    private static List<String> normalizeList(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String value : values) {
            String normalized = normalize(value, null);
            if (normalized != null) {
                result.add(normalized);
            }
        }
        return result;
    }

    private static String firstOrDefault(List<String> values, String defaultValue) {
        if (values == null || values.isEmpty()) {
            return defaultValue;
        }
        return normalize(values.get(0), defaultValue);
    }
}
