package com.jmqx.admin.embedded;

import com.jmqx.common.logging.ClientTraceManager;

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
                SecurityConfig.defaultConfig(),
                BridgeConfig.defaults()
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
    public BridgeConfig getBridgeConfig(String clusterId) {
        return getOrCreate(clusterId).bridgeConfig;
    }

    @Override
    public void setBridgeConfig(String clusterId, BridgeConfig bridgeConfig) {
        ClusterState state = getOrCreate(clusterId);
        state.bridgeConfig = bridgeConfig;
        state.bridgeConfigInitialized = true;
    }

    @Override
    public boolean hasBridgeConfig(String clusterId) {
        return getOrCreate(clusterId).bridgeConfigInitialized;
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
    public void upsertClientSnapshot(String clusterId, ClientSnapshot clientSnapshot) {
        if (clientSnapshot == null || clientSnapshot.clientId() == null || clientSnapshot.clientId().isBlank()) {
            return;
        }
        getOrCreate(clusterId).clientSnapshots.put(clientSnapshot.clientId(), clientSnapshot);
    }

    @Override
    public void removeClientSnapshot(String clusterId, String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        getOrCreate(clusterId).clientSnapshots.remove(clientId);
    }

    @Override
    public void replaceClientSubscriptions(String clusterId, String clientId, List<String> topics) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        ClusterState state = getOrCreate(clusterId);
        ClientSnapshot existing = state.clientSnapshots.get(clientId);
        if (existing == null) {
            return;
        }
        state.clientSnapshots.put(existing.clientId(), new ClientSnapshot(
                existing.clientId(),
                existing.nodeId(),
                existing.clientIp(),
                existing.keepAliveSeconds(),
                existing.connectionType(),
                existing.username(),
                existing.connectedAt(),
                normalizeList(topics)
        ));
    }

    @Override
    public List<ClientSnapshot> listClientSnapshots(String clusterId) {
        List<ClientSnapshot> clients = new ArrayList<>(getOrCreate(clusterId).clientSnapshots.values());
        clients.sort(Comparator.comparing(ClientSnapshot::connectedAt).reversed());
        return clients;
    }

    @Override
    public ClientSnapshot getClientSnapshot(String clusterId, String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return null;
        }
        return getOrCreate(clusterId).clientSnapshots.get(clientId);
    }

    @Override
    public void upsertBlacklistEntry(String clusterId, BlacklistEntry entry) {
        if (entry == null || entry.value() == null || entry.value().isBlank()) {
            return;
        }
        getOrCreate(clusterId).blacklistEntries.put(blacklistKey(entry.type(), entry.value()), entry);
    }

    @Override
    public void removeBlacklistEntry(String clusterId, String type, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        getOrCreate(clusterId).blacklistEntries.remove(blacklistKey(type, value));
    }

    @Override
    public List<BlacklistEntry> listBlacklistEntries(String clusterId) {
        List<BlacklistEntry> entries = new ArrayList<>(getOrCreate(clusterId).blacklistEntries.values());
        entries.sort(Comparator.comparing(BlacklistEntry::createdAt).reversed());
        return entries;
    }

    @Override
    public void upsertClientTraceTask(String clusterId, ClientTraceManager.ClientTraceTask task) {
        if (task == null || task.id() == null || task.id().isBlank()) {
            return;
        }
        getOrCreate(clusterId).clientTraceTasks.put(task.id(), task.normalize());
    }

    @Override
    public void removeClientTraceTask(String clusterId, String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        getOrCreate(clusterId).clientTraceTasks.remove(taskId);
    }

    @Override
    public List<ClientTraceManager.ClientTraceTask> listClientTraceTasks(String clusterId) {
        List<ClientTraceManager.ClientTraceTask> tasks = new ArrayList<>(getOrCreate(clusterId).clientTraceTasks.values());
        tasks.sort(Comparator.comparing(ClientTraceManager.ClientTraceTask::startAt).reversed());
        return tasks;
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
                SecurityConfig.defaultConfig(),
                BridgeConfig.defaults()
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
            boolean aclDefaultAllow,
            AclHttpConfig aclHttp,
            AclFileConfig aclFile,
            AclRedisConfig aclRedis,
            boolean authEnabled,
            List<String> authChain,
            long cacheTtlMs,
            AuthHttpConfig authHttp,
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
                    false,
                    AclHttpConfig.defaults(),
                    AclFileConfig.defaults(),
                    AclRedisConfig.defaults(),
                    authEnabled,
                    authChain,
                    cacheTtlMs,
                    AuthHttpConfig.defaults(),
                    AuthBuiltInDatabaseConfig.defaults(),
                    AuthRedisConfig.defaults(),
                    AuthMysqlConfig.defaults(),
                    AuthPostgresqlConfig.defaults()
            );
        }

        public SecurityConfig(boolean aclEnabled,
                              List<String> aclChain,
                              boolean authEnabled,
                              List<String> authChain,
                              long cacheTtlMs,
                              AuthHttpConfig authHttp,
                              AuthBuiltInDatabaseConfig authBuiltInDatabase,
                              AuthRedisConfig authRedis,
                              AuthMysqlConfig authMysql,
                              AuthPostgresqlConfig authPostgresql) {
            this(
                    aclEnabled,
                    aclChain,
                    false,
                    AclHttpConfig.defaults(),
                    AclFileConfig.defaults(),
                    AclRedisConfig.defaults(),
                    authEnabled,
                    authChain,
                    cacheTtlMs,
                    authHttp,
                    authBuiltInDatabase,
                    authRedis,
                    authMysql,
                    authPostgresql
            );
        }

        public SecurityConfig(boolean aclEnabled,
                              List<String> aclChain,
                              boolean authEnabled,
                              List<String> authChain,
                              long cacheTtlMs,
                              AuthHttpConfig authHttp,
                              AuthFileConfig authFile,
                              AuthBuiltInDatabaseConfig authBuiltInDatabase,
                              AuthRedisConfig authRedis,
                              AuthMysqlConfig authMysql,
                              AuthPostgresqlConfig authPostgresql) {
            this(aclEnabled, aclChain, authEnabled, authChain, cacheTtlMs, authHttp, authBuiltInDatabase, authRedis, authMysql, authPostgresql);
        }

        public SecurityConfig(boolean aclEnabled,
                              List<String> aclChain,
                              boolean aclDefaultAllow,
                              AclHttpConfig aclHttp,
                              AclFileConfig aclFile,
                              AclRedisConfig aclRedis,
                              boolean authEnabled,
                              List<String> authChain,
                              long cacheTtlMs) {
            this(
                    aclEnabled,
                    aclChain,
                    aclDefaultAllow,
                    aclHttp,
                    aclFile,
                    aclRedis,
                    authEnabled,
                    authChain,
                    cacheTtlMs,
                    AuthHttpConfig.defaults(),
                    AuthBuiltInDatabaseConfig.defaults(),
                    AuthRedisConfig.defaults(),
                    AuthMysqlConfig.defaults(),
                    AuthPostgresqlConfig.defaults()
            );
        }

        public SecurityConfig(boolean aclEnabled,
                              List<String> aclChain,
                              boolean aclDefaultAllow,
                              AclHttpConfig aclHttp,
                              AclFileConfig aclFile,
                              AclRedisConfig aclRedis,
                              boolean authEnabled,
                              List<String> authChain,
                              long cacheTtlMs,
                              AuthHttpConfig authHttp,
                              AuthFileConfig authFile,
                              AuthBuiltInDatabaseConfig authBuiltInDatabase,
                              AuthRedisConfig authRedis,
                              AuthMysqlConfig authMysql,
                              AuthPostgresqlConfig authPostgresql) {
            this(
                    aclEnabled,
                    aclChain,
                    aclDefaultAllow,
                    aclHttp,
                    aclFile,
                    aclRedis,
                    authEnabled,
                    authChain,
                    cacheTtlMs,
                    authHttp,
                    authBuiltInDatabase,
                    authRedis,
                    authMysql,
                    authPostgresql
            );
        }

        public SecurityConfig {
            aclChain = normalizePluginChain(aclChain);
            authChain = normalizePluginChain(authChain);
            aclHttp = aclHttp == null ? AclHttpConfig.defaults() : aclHttp;
            aclFile = aclFile == null ? AclFileConfig.defaults() : aclFile;
            aclRedis = aclRedis == null ? AclRedisConfig.defaults() : aclRedis;
            authHttp = authHttp == null ? AuthHttpConfig.defaults() : authHttp;
            authBuiltInDatabase = authBuiltInDatabase == null ? AuthBuiltInDatabaseConfig.defaults() : authBuiltInDatabase;
            authRedis = authRedis == null ? AuthRedisConfig.defaults() : authRedis;
            authMysql = authMysql == null ? AuthMysqlConfig.defaults() : authMysql;
            authPostgresql = authPostgresql == null ? AuthPostgresqlConfig.defaults() : authPostgresql;
        }

        public static SecurityConfig defaultConfig() {
            return new SecurityConfig(
                    false,
                    List.of(),
                    false,
                    AclHttpConfig.defaults(),
                    AclFileConfig.defaults(),
                    AclRedisConfig.defaults(),
                    false,
                    List.of(),
                    60_000,
                    AuthHttpConfig.defaults(),
                    AuthBuiltInDatabaseConfig.defaults(),
                    AuthRedisConfig.defaults(),
                    AuthMysqlConfig.defaults(),
                    AuthPostgresqlConfig.defaults()
            );
        }
    }

    public record AclHttpConfig(String url, int timeoutMs, String bodyTemplate) {
        public AclHttpConfig {
            url = normalize(url, "http://127.0.0.1:8080/acl/check");
            timeoutMs = Math.max(timeoutMs, 200);
            bodyTemplate = bodyTemplate == null || bodyTemplate.isBlank()
                    ? """
                    {
                      "clientId": "${clientId}",
                      "username": "${username}",
                      "topic": "${topic}",
                      "action": "${action}"
                    }
                    """
                    : bodyTemplate;
        }

        public static AclHttpConfig defaults() {
            return new AclHttpConfig(
                    "http://127.0.0.1:8080/acl/check",
                    2000,
                    """
                    {
                      "clientId": "${clientId}",
                      "username": "${username}",
                      "topic": "${topic}",
                      "action": "${action}"
                    }
                    """
            );
        }
    }

    public record AclFileConfig(String path) {
        public AclFileConfig {
            path = normalize(path, "acl-rules.txt");
        }

        public static AclFileConfig defaults() {
            return new AclFileConfig("acl-rules.txt");
        }
    }

    public record AclRedisConfig(String host, int port, String password, int db, String keyPrefix, int timeoutMs) {
        public AclRedisConfig {
            host = normalize(host, "127.0.0.1");
            password = password == null ? "" : password;
            keyPrefix = normalize(keyPrefix, "jmqx:acl");
            timeoutMs = Math.max(timeoutMs, 200);
        }

        public static AclRedisConfig defaults() {
            return new AclRedisConfig("127.0.0.1", 6379, "", 0, "jmqx:acl", 2000);
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

    public record BridgeConfig(
            boolean enabled,
            List<String> types,
            List<String> topicFilters,
            boolean asyncEnabled,
            int asyncQueueCapacity,
            int asyncWorkerCount,
            BridgeKafkaConfig kafka,
            BridgeRocketmqConfig rocketmq,
            BridgeMysqlConfig mysql
    ) {
        public BridgeConfig {
            types = normalizeBridgeTypes(types);
            topicFilters = normalizeList(topicFilters);
            asyncQueueCapacity = Math.max(1024, asyncQueueCapacity);
            asyncWorkerCount = Math.max(1, asyncWorkerCount);
            kafka = kafka == null ? BridgeKafkaConfig.defaults() : kafka;
            rocketmq = rocketmq == null ? BridgeRocketmqConfig.defaults() : rocketmq;
            mysql = mysql == null ? BridgeMysqlConfig.defaults() : mysql;
        }

        public static BridgeConfig defaults() {
            return new BridgeConfig(
                    false,
                    List.of(),
                    List.of(),
                    true,
                    10_000,
                    1,
                    BridgeKafkaConfig.defaults(),
                    BridgeRocketmqConfig.defaults(),
                    BridgeMysqlConfig.defaults()
            );
        }
    }

    public record BridgeKafkaConfig(
            boolean enabled,
            String bootstrapServers,
            String topic,
            List<String> sourceTopicFilters,
            String acks,
            String clientId,
            String compressionType
    ) {
        public BridgeKafkaConfig {
            bootstrapServers = normalize(bootstrapServers, "127.0.0.1:9092");
            topic = normalize(topic, "jmqx-messages");
            sourceTopicFilters = normalizeList(sourceTopicFilters);
            acks = normalize(acks, "1");
            clientId = normalize(clientId, "jmqx-bridge");
            compressionType = normalize(compressionType, "none");
        }

        public static BridgeKafkaConfig defaults() {
            return new BridgeKafkaConfig(
                    false,
                    "127.0.0.1:9092",
                    "jmqx-messages",
                    List.of(),
                    "1",
                    "jmqx-bridge",
                    "none"
            );
        }
    }

    public record BridgeRocketmqConfig(
            boolean enabled,
            String nameServer,
            String producerGroup,
            String topic,
            List<String> sourceTopicFilters,
            boolean syncSend,
            int timeoutMs
    ) {
        public BridgeRocketmqConfig {
            nameServer = normalize(nameServer, "127.0.0.1:9876");
            producerGroup = normalize(producerGroup, "jmqx-bridge-group");
            topic = normalize(topic, "JMQX_MESSAGES");
            sourceTopicFilters = normalizeList(sourceTopicFilters);
            timeoutMs = Math.max(100, timeoutMs);
        }

        public static BridgeRocketmqConfig defaults() {
            return new BridgeRocketmqConfig(
                    false,
                    "127.0.0.1:9876",
                    "jmqx-bridge-group",
                    "JMQX_MESSAGES",
                    List.of(),
                    false,
                    3000
            );
        }
    }

    public record BridgeMysqlConfig(
            boolean enabled,
            String driver,
            String url,
            String user,
            String password,
            String table,
            List<String> sourceTopicFilters,
            boolean autoCreateTable,
            int poolMinIdle,
            int poolMaxSize,
            long poolConnectionTimeoutMs,
            long poolIdleTimeoutMs,
            long poolMaxLifetimeMs
    ) {
        public BridgeMysqlConfig {
            driver = driver == null ? "" : driver.trim();
            url = normalize(url, "jdbc:mysql://127.0.0.1:3306/jmqx");
            user = normalize(user, "root");
            password = password == null ? "" : password;
            table = normalize(table, "jmqx_bridge_message");
            sourceTopicFilters = normalizeList(sourceTopicFilters);
            poolMinIdle = Math.max(0, poolMinIdle);
            poolMaxSize = Math.max(1, poolMaxSize);
            if (poolMinIdle > poolMaxSize) {
                poolMinIdle = poolMaxSize;
            }
            poolConnectionTimeoutMs = Math.max(250L, poolConnectionTimeoutMs);
            poolIdleTimeoutMs = Math.max(10_000L, poolIdleTimeoutMs);
            poolMaxLifetimeMs = Math.max(30_000L, poolMaxLifetimeMs);
        }

        public static BridgeMysqlConfig defaults() {
            return new BridgeMysqlConfig(
                    false,
                    "",
                    "jdbc:mysql://127.0.0.1:3306/jmqx",
                    "root",
                    "",
                    "jmqx_bridge_message",
                    List.of(),
                    true,
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
            long reportTime,
            long connectAuthSuccess,
            long connectAuthFailure,
            long connectAuthError,
            long connectAuthSlow,
            long connectAuthAvgMs,
            long connectAuthMaxMs,
            long publishAclAllow,
            long publishAclDeny,
            long publishAclError,
            long publishAclSlow,
            long publishAclAvgMs,
            long publishAclMaxMs
    ) {
    }

    /**
     * 客户端会话快照。
     */
    public record ClientSnapshot(
            String clientId,
            String nodeId,
            String clientIp,
            int keepAliveSeconds,
            String connectionType,
            String username,
            long connectedAt,
            List<String> subscribedTopics
    ) {
        public ClientSnapshot {
            clientId = normalize(clientId, "");
            nodeId = normalize(nodeId, "unknown");
            clientIp = normalize(clientIp, "unknown");
            connectionType = normalize(connectionType, "");
            username = normalize(username, "");
            keepAliveSeconds = Math.max(0, keepAliveSeconds);
            connectedAt = Math.max(0L, connectedAt);
            subscribedTopics = normalizeList(subscribedTopics);
        }

    }

    /**
     * 黑名单条目。
     */
    public record BlacklistEntry(
            String type,
            String value,
            long createdAt,
            String source
    ) {
        public BlacklistEntry {
            type = normalizeBlacklistType(type);
            value = normalize(value, "");
            createdAt = Math.max(0L, createdAt);
            source = normalize(source, "");
        }
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
        private volatile BridgeConfig bridgeConfig;
        private volatile boolean clusterConfigInitialized;
        private volatile boolean securityConfigInitialized;
        private volatile boolean bridgeConfigInitialized;
        private final Map<String, NodeMetrics> nodeMetrics = new ConcurrentHashMap<>();
        private final Map<String, ClientSnapshot> clientSnapshots = new ConcurrentHashMap<>();
        private final Map<String, BlacklistEntry> blacklistEntries = new ConcurrentHashMap<>();
        private final Map<String, ClientTraceManager.ClientTraceTask> clientTraceTasks = new ConcurrentHashMap<>();
        private final List<AuditLogEntry> auditLogs = new CopyOnWriteArrayList<>();

        private ClusterState(ClusterSummary summary, ClusterConfig clusterConfig, SecurityConfig securityConfig, BridgeConfig bridgeConfig) {
            this.summary = summary;
            this.clusterConfig = clusterConfig;
            this.securityConfig = securityConfig;
            this.bridgeConfig = bridgeConfig;
            this.clusterConfigInitialized = false;
            this.securityConfigInitialized = false;
            this.bridgeConfigInitialized = false;
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

    private static List<String> normalizePluginChain(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String value : values) {
            String normalized = normalize(value, null);
            if (normalized == null) {
                continue;
            }
            normalized = normalized.toLowerCase();
            if ("allow_all".equals(normalized) || result.contains(normalized)) {
                continue;
            }
            result.add(normalized);
        }
        return result;
    }

    private static List<String> normalizeBridgeTypes(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String value : values) {
            String normalized = normalize(value, null);
            if (normalized == null) {
                continue;
            }
            normalized = normalized.toLowerCase();
            if (!List.of("kafka", "rocketmq", "mysql").contains(normalized) || result.contains(normalized)) {
                continue;
            }
            result.add(normalized);
        }
        return result;
    }

    private static String firstOrDefault(List<String> values, String defaultValue) {
        if (values == null || values.isEmpty()) {
            return defaultValue;
        }
        return normalize(values.get(0), defaultValue);
    }

    private static String normalizeBlacklistType(String type) {
        String normalized = normalize(type, "clientId");
        return "ip".equalsIgnoreCase(normalized) ? "ip" : "clientId";
    }

    private static String blacklistKey(String type, String value) {
        return normalizeBlacklistType(type) + ":" + normalize(value, "");
    }
}
