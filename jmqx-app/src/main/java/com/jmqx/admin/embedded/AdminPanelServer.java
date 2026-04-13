package com.jmqx.admin.embedded;

import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.session.ClientSession;
import com.jmqx.session.SessionRegistry;
import com.jmqx.transport.ConnectionMetrics;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * jmqx 内嵌管理页面服务。
 * 页面资源由 jmqx-app 提供，API 由 jmqx-app 内嵌实现。
 *
 * @author liucaiwen
 * @since 2026-04-10
 */
public class AdminPanelServer {

    private static final Logger LOG = Logger.getLogger(AdminPanelServer.class.getName());

    private final String host;
    private final int port;
    private final String basePath;
    private final String backendBaseUrl;
    private final String defaultClusterId;
    private final String nodeId;
    private final String nodeIp;
    private final String nodeRole;
    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private final ConnectionMetrics connectionMetrics;
    private final AdminStateRepository stateStore;
    private final BuiltInDatabaseUserService builtInDatabaseUserService;
    private final SecurityConfigUpdater securityConfigUpdater;
    private final ClusterConfigUpdater clusterConfigUpdater;
    private final BuiltInDatabaseUsersUpdater builtInDatabaseUsersUpdater;
    private final HttpClient httpClient;
    private HttpServer server;

    public AdminPanelServer(String host,
                            int port,
                            String basePath,
                            String backendBaseUrl,
                            String defaultClusterId,
                            String nodeId,
                            String nodeIp,
                            String nodeRole,
                            SessionRegistry sessionRegistry,
                            SubscriptionRegistry subscriptionRegistry,
                            ConnectionMetrics connectionMetrics,
                            AdminStateRepository stateStore,
                            EmbeddedAdminStateStore.SecurityConfig initialSecurityConfig,
                            EmbeddedAdminStateStore.ClusterConfig initialClusterConfig,
                            SecurityConfigUpdater securityConfigUpdater,
                            ClusterConfigUpdater clusterConfigUpdater,
                            BuiltInDatabaseUsersUpdater builtInDatabaseUsersUpdater,
                            BuiltInDatabaseUserService builtInDatabaseUserService) {
        this.host = (host == null || host.isBlank()) ? "0.0.0.0" : host.trim();
        this.port = port <= 0 ? 18081 : port;
        this.basePath = normalizeBasePath(basePath);
        this.backendBaseUrl = stripTrailingSlash(backendBaseUrl == null || backendBaseUrl.isBlank()
                ? "http://127.0.0.1:18080"
                : backendBaseUrl.trim());
        this.defaultClusterId = normalize(defaultClusterId, "default");
        this.nodeId = normalize(nodeId, "node-1");
        this.nodeIp = normalize(nodeIp, "unknown");
        this.nodeRole = normalize(nodeRole, "CORE");
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.connectionMetrics = connectionMetrics;
        this.stateStore = stateStore == null ? new EmbeddedAdminStateStore() : stateStore;
        this.builtInDatabaseUserService = builtInDatabaseUserService == null ? new BuiltInDatabaseUserService() : builtInDatabaseUserService;
        this.stateStore.createCluster(this.defaultClusterId, "默认集群", this.nodeIp + ":7800");
        if (initialSecurityConfig != null && !this.stateStore.hasSecurityConfig(this.defaultClusterId)) {
            this.stateStore.setSecurityConfig(this.defaultClusterId, initialSecurityConfig);
        }
        if (initialClusterConfig != null && !this.stateStore.hasClusterConfig(this.defaultClusterId)) {
            this.stateStore.setClusterConfig(this.defaultClusterId, initialClusterConfig);
        }
        this.securityConfigUpdater = securityConfigUpdater == null ? (clusterId, config) -> {
        } : securityConfigUpdater;
        this.clusterConfigUpdater = clusterConfigUpdater == null ? (clusterId, config) -> {
        } : clusterConfigUpdater;
        this.builtInDatabaseUsersUpdater = builtInDatabaseUsersUpdater == null ? new BuiltInDatabaseUsersUpdater() {
            @Override
            public void upsert(String clusterId, EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config, String userId, String password, boolean superuser) {
                AdminPanelServer.this.builtInDatabaseUserService.upsertUser(config, userId, password, superuser);
            }

            @Override
            public int importUsers(String clusterId, EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config, List<BuiltInDatabaseUserService.UserInput> users) {
                return AdminPanelServer.this.builtInDatabaseUserService.importUsers(config, users);
            }

            @Override
            public void delete(String clusterId, String userId) {
                AdminPanelServer.this.builtInDatabaseUserService.deleteUser(userId);
            }

            @Override
            public void deleteAll(String clusterId) {
                AdminPanelServer.this.builtInDatabaseUserService.deleteAllUsers();
            }
        } : builtInDatabaseUsersUpdater;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    public void start() {
        if (server != null) {
            return;
        }
        try {
            server = HttpServer.create(new InetSocketAddress(host, port), 0);
            server.createContext("/", new RootHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            LOG.info(() -> "[ADMIN-PANEL] started at http://127.0.0.1:" + port + basePath
                    + ", backend=" + backendBaseUrl);
        } catch (IOException exception) {
            throw new RuntimeException("failed to start admin panel server", exception);
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
        stateStore.close();
    }

    private final class RootHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                if (isApiPath(path)) {
                    handleApi(exchange);
                    return;
                }
                if ("/".equals(path)) {
                    redirect(exchange, basePath + "/");
                    return;
                }
                if (path.startsWith(basePath)) {
                    serveStatic(exchange, path);
                    return;
                }
                if (path.startsWith("/webjars/")) {
                    proxy(exchange);
                    return;
                }
                write(exchange, 404, "text/plain; charset=UTF-8", "Not Found".getBytes(StandardCharsets.UTF_8));
            } catch (Exception exception) {
                byte[] body = ("Admin panel error: " + exception.getMessage()).getBytes(StandardCharsets.UTF_8);
                write(exchange, 500, "text/plain; charset=UTF-8", body);
            }
        }
    }

    private void serveStatic(HttpExchange exchange, String rawPath) throws IOException {
        String resourcePath = rawPath.substring(basePath.length());
        if (resourcePath.isBlank() || "/".equals(resourcePath)) {
            resourcePath = "/index.html";
        }
        InputStream stream = AdminPanelServer.class.getClassLoader()
                .getResourceAsStream("admin/static" + resourcePath);
        if (stream == null) {
            write(exchange, 404, "text/plain; charset=UTF-8", "Not Found".getBytes(StandardCharsets.UTF_8));
            return;
        }
        byte[] bytes = readAll(stream);
        write(exchange, 200, contentType(resourcePath), bytes);
    }

    private void handleApi(HttpExchange exchange) throws IOException {
        String path = normalizeApiPath(exchange.getRequestURI().getPath());
        String method = exchange.getRequestMethod().toUpperCase();
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        String clusterId = normalize(query.get("clusterId"), defaultClusterId);
        byte[] requestBody = readAll(exchange.getRequestBody());
        String body = new String(requestBody, StandardCharsets.UTF_8);

        if ("/api/v1/clusters".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, toClustersJson(stateStore.listClusters()));
            return;
        }
        if ("/api/v1/clusters".equals(path) && "POST".equals(method)) {
            String newClusterId = normalize(extractString(body, "clusterId"), null);
            String displayName = normalize(extractString(body, "displayName"), null);
            String seedCoreNode = normalize(extractString(body, "seedCoreNode"), null);
            if (newClusterId == null || displayName == null || seedCoreNode == null) {
                writeJson(exchange, 400, "{\"error\":\"invalid create cluster request\"}");
                return;
            }
            EmbeddedAdminStateStore.ClusterSummary created = stateStore.createCluster(newClusterId, displayName, seedCoreNode);
            writeJson(exchange, 200, toClusterJson(created));
            return;
        }
        if ("/api/v1/cluster/overview".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, buildOverviewJson(clusterId));
            return;
        }
        if (path.startsWith("/api/v1/internal/nodes/") && path.endsWith("/metrics") && "POST".equals(method)) {
            String nodeId = decode(path.substring("/api/v1/internal/nodes/".length(), path.length() - "/metrics".length()));
            EmbeddedAdminStateStore.NodeMetrics metrics = parseNodeMetrics(nodeId, body, clusterId);
            stateStore.upsertNodeMetrics(clusterId, metrics);
            writeJson(exchange, 200, "{\"ok\":true}");
            return;
        }
        if ("/api/v1/cluster/config".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, toClusterConfigJson(stateStore.getClusterConfig(clusterId)));
            return;
        }
        if ("/api/v1/cluster/config".equals(path) && "PUT".equals(method)) {
            EmbeddedAdminStateStore.ClusterConfig before = stateStore.getClusterConfig(clusterId);
            EmbeddedAdminStateStore.ClusterConfig config = parseClusterConfig(body, before);
            clusterConfigUpdater.apply(clusterId, config);
            stateStore.setClusterConfig(clusterId, config);
            appendAuditLog(
                    clusterId,
                    "cluster.config.updated",
                    resolveAuditSource(exchange),
                    toClusterConfigJson(before),
                    toClusterConfigJson(config)
            );
            writeJson(exchange, 200, toClusterConfigJson(config));
            return;
        }
        if ("/api/v1/cluster/full-config".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, toFullConfigJson(stateStore.getFullConfig(clusterId)));
            return;
        }
        if ("/api/v1/audit/logs".equals(path) && "GET".equals(method)) {
            int limit = Math.min(Math.max(parseInt(query.get("limit"), 20), 1), 200);
            writeJson(exchange, 200, toAuditLogsJson(stateStore.listAuditLogs(clusterId, limit)));
            return;
        }
        if ("/api/v1/security/config".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, toSecurityConfigJson(stateStore.getSecurityConfig(clusterId)));
            return;
        }
        if ("/api/v1/auth/built-in/users".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, buildBuiltInUsersJson(clusterId));
            return;
        }
        if ("/api/v1/auth/built-in/users".equals(path) && "DELETE".equals(method)) {
            builtInDatabaseUsersUpdater.deleteAll(clusterId);
            writeJson(exchange, 200, buildBuiltInUsersJson(clusterId));
            return;
        }
        if ("/api/v1/auth/built-in/users".equals(path) && "POST".equals(method)) {
            EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config = stateStore.getSecurityConfig(clusterId).authBuiltInDatabase();
            String userId = normalize(extractString(body, "userId"), "");
            String password = normalize(extractString(body, "password"), "");
            boolean superuser = extractBoolean(body, "superuser", false);
            if (userId.isBlank() || password.isBlank()) {
                writeJson(exchange, 400, "{\"error\":\"userId and password are required\"}");
                return;
            }
            builtInDatabaseUsersUpdater.upsert(clusterId, config, userId, password, superuser);
            writeJson(exchange, 200, buildBuiltInUsersJson(clusterId));
            return;
        }
        if ("/api/v1/auth/built-in/users/import".equals(path) && "POST".equals(method)) {
            EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config = stateStore.getSecurityConfig(clusterId).authBuiltInDatabase();
            List<String> lines = extractStringList(body, "lines");
            List<BuiltInDatabaseUserService.UserInput> users = new ArrayList<>();
            for (String line : lines) {
                if (line == null || line.isBlank()) {
                    continue;
                }
                String[] parts = line.split(",", 3);
                if (parts.length < 2) {
                    continue;
                }
                users.add(new BuiltInDatabaseUserService.UserInput(
                        parts[0].trim(),
                        parts[1].trim(),
                        parts.length >= 3 && Boolean.parseBoolean(parts[2].trim())
                ));
            }
            int imported = builtInDatabaseUsersUpdater.importUsers(clusterId, config, users);
            writeJson(exchange, 200, "{\"imported\":" + imported + ",\"data\":" + buildBuiltInUsersJson(clusterId) + "}");
            return;
        }
        if (path.startsWith("/api/v1/auth/built-in/users/") && "DELETE".equals(method)) {
            String userId = decode(path.substring("/api/v1/auth/built-in/users/".length()));
            builtInDatabaseUsersUpdater.delete(clusterId, userId);
            writeJson(exchange, 200, buildBuiltInUsersJson(clusterId));
            return;
        }
        if ("/api/v1/security/config".equals(path) && "PUT".equals(method)) {
            EmbeddedAdminStateStore.SecurityConfig before = stateStore.getSecurityConfig(clusterId);
            EmbeddedAdminStateStore.SecurityConfig config = parseSecurityConfig(body, before);
            String validationError = validateSecurityConfig(config);
            if (validationError != null) {
                writeJson(exchange, 400, "{\"error\":\"" + escape(validationError) + "\"}");
                return;
            }
            securityConfigUpdater.apply(clusterId, config);
            stateStore.setSecurityConfig(clusterId, config);
            appendAuditLog(
                    clusterId,
                    "security.config.updated",
                    resolveAuditSource(exchange),
                    toSecurityConfigJson(before),
                    toSecurityConfigJson(config)
            );
            writeJson(exchange, 200, toSecurityConfigJson(config));
            return;
        }
        if ("/api/v1/clients".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, buildClientsJson(query));
            return;
        }
        if (path.startsWith("/api/v1/clients/") && "GET".equals(method)) {
            String clientId = decode(path.substring("/api/v1/clients/".length()));
            String detailJson = buildClientDetailJson(clientId);
            if (detailJson == null) {
                writeJson(exchange, 404, "{\"error\":\"client not found\"}");
            } else {
                writeJson(exchange, 200, detailJson);
            }
            return;
        }
        writeJson(exchange, 404, "{\"error\":\"not found\"}");
    }

    private boolean isApiPath(String path) {
        if (path == null || path.isBlank()) {
            return false;
        }
        return path.startsWith("/api/v1/")
                || "/api/v1".equals(path)
                || path.startsWith(basePath + "/api/v1/")
                || (basePath + "/api/v1").equals(path);
    }

    private String normalizeApiPath(String path) {
        if (path == null || path.isBlank()) {
            return path;
        }
        String prefixedApiPath = basePath + "/api/v1";
        if (path.equals(prefixedApiPath)) {
            return "/api/v1";
        }
        if (path.startsWith(prefixedApiPath + "/")) {
            return path.substring(basePath.length());
        }
        return path;
    }

    private void proxy(HttpExchange exchange) throws IOException {
        URI uri = exchange.getRequestURI();
        String target = backendBaseUrl + uri.getPath();
        if (uri.getQuery() != null && !uri.getQuery().isBlank()) {
            target += "?" + uri.getQuery();
        }
        byte[] requestBody = readAll(exchange.getRequestBody());
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(target))
                .timeout(Duration.ofSeconds(5));

        String method = exchange.getRequestMethod();
        if ("GET".equalsIgnoreCase(method)) {
            builder.GET();
        } else if ("DELETE".equalsIgnoreCase(method)) {
            builder.method("DELETE", HttpRequest.BodyPublishers.ofByteArray(requestBody));
        } else {
            builder.method(method, HttpRequest.BodyPublishers.ofByteArray(requestBody));
        }

        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType != null && !contentType.isBlank()) {
            builder.header("Content-Type", contentType);
        }

        try {
            HttpResponse<byte[]> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            Headers headers = exchange.getResponseHeaders();
            for (Map.Entry<String, java.util.List<String>> entry : response.headers().map().entrySet()) {
                if ("transfer-encoding".equalsIgnoreCase(entry.getKey())) {
                    continue;
                }
                headers.put(entry.getKey(), entry.getValue());
            }
            write(exchange, response.statusCode(), headers.getFirst("Content-Type"), response.body());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            write(exchange, 502, "text/plain; charset=UTF-8",
                    ("Proxy interrupted: " + exception.getMessage()).getBytes(StandardCharsets.UTF_8));
        } catch (Exception exception) {
            write(exchange, 502, "text/plain; charset=UTF-8",
                    ("Proxy failed: " + exception.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void redirect(HttpExchange exchange, String location) throws IOException {
        exchange.getResponseHeaders().set("Location", location);
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
    }

    private static void write(HttpExchange exchange, int code, String contentType, byte[] body) throws IOException {
        if (contentType != null && !contentType.isBlank()) {
            exchange.getResponseHeaders().set("Content-Type", contentType);
        }
        exchange.sendResponseHeaders(code, body.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        }
    }

    private static String contentType(String path) {
        if (path.endsWith(".html")) {
            return "text/html; charset=UTF-8";
        }
        if (path.endsWith(".js")) {
            return "application/javascript; charset=UTF-8";
        }
        if (path.endsWith(".css")) {
            return "text/css; charset=UTF-8";
        }
        if (path.endsWith(".json")) {
            return "application/json; charset=UTF-8";
        }
        return "application/octet-stream";
    }

    private String buildOverviewJson(String clusterId) {
        upsertLocalNodeSnapshot(clusterId);
        List<EmbeddedAdminStateStore.NodeMetrics> nodes = stateStore.listNodeMetrics(clusterId);
        int connections = 0;
        long inbound = 0;
        long outbound = 0;
        StringBuilder nodeJson = new StringBuilder("[");
        for (int i = 0; i < nodes.size(); i++) {
            EmbeddedAdminStateStore.NodeMetrics node = nodes.get(i);
            if (i > 0) {
                nodeJson.append(",");
            }
            connections += Math.max(0, node.connectedClients());
            inbound += Math.max(0, node.inboundBytes());
            outbound += Math.max(0, node.outboundBytes());
            nodeJson.append(toNodeMetricsJson(node));
        }
        nodeJson.append("]");
        return "{"
                + "\"clusterId\":\"" + escape(clusterId) + "\","
                + "\"totalConnections\":" + connections + ","
                + "\"totalInboundBytes\":" + inbound + ","
                + "\"totalOutboundBytes\":" + outbound + ","
                + "\"nodes\":" + nodeJson
                + "}";
    }

    private void upsertLocalNodeSnapshot(String clusterId) {
        int connections = connectionMetrics == null ? 0 : connectionMetrics.getActiveConnections();
        long inbound = connectionMetrics == null ? 0 : connectionMetrics.getInboundBytes();
        long outbound = connectionMetrics == null ? 0 : connectionMetrics.getOutboundBytes();
        EmbeddedAdminStateStore.NodeMetrics localMetrics = new EmbeddedAdminStateStore.NodeMetrics(
                nodeId,
                nodeIp,
                nodeRole,
                inbound,
                outbound,
                connections,
                System.currentTimeMillis()
        );
        stateStore.upsertNodeMetrics(clusterId, localMetrics);
    }

    private String buildClientsJson(Map<String, String> query) {
        String clientIdFilter = normalize(query.get("clientId"), "");
        String userNameFilter = normalize(query.get("userName"), "");
        int pageNo = parseInt(query.get("pageNo"), 1);
        int pageSize = Math.min(Math.max(parseInt(query.get("pageSize"), 20), 1), 200);
        List<ClientSession> sessions = sessionRegistry == null ? List.of() : sessionRegistry.list();
        List<ClientSession> filtered = new ArrayList<>();
        for (ClientSession session : sessions) {
            if (session == null) {
                continue;
            }
            if (!containsIgnoreCase(session.clientId(), clientIdFilter)) {
                continue;
            }
            if (!containsIgnoreCase(session.username(), userNameFilter)) {
                continue;
            }
            filtered.add(session);
        }
        filtered.sort(Comparator.comparing(ClientSession::connectedAt).reversed());
        int from = Math.max((pageNo - 1) * pageSize, 0);
        int to = Math.min(from + pageSize, filtered.size());
        List<ClientSession> page = from >= filtered.size() ? List.of() : filtered.subList(from, to);

        StringBuilder records = new StringBuilder();
        for (int i = 0; i < page.size(); i++) {
            if (i > 0) {
                records.append(",");
            }
            records.append(toClientRowJson(page.get(i)));
        }
        return "{"
                + "\"records\":[" + records + "],"
                + "\"total\":" + filtered.size() + ","
                + "\"pageNo\":" + pageNo + ","
                + "\"pageSize\":" + pageSize
                + "}";
    }

    private String buildClientDetailJson(String clientId) {
        if (sessionRegistry == null || clientId == null || clientId.isBlank()) {
            return null;
        }
        ClientSession session = sessionRegistry.get(clientId).orElse(null);
        if (session == null) {
            return null;
        }
        Set<String> topics = subscriptionRegistry == null
                ? Set.of()
                : new LinkedHashSet<>(subscriptionRegistry.findSubscriptions(clientId).keySet());
        StringBuilder topicsJson = new StringBuilder();
        int i = 0;
        for (String topic : topics) {
            if (i++ > 0) {
                topicsJson.append(",");
            }
            topicsJson.append("\"").append(escape(topic)).append("\"");
        }
        return "{"
                + "\"session\":" + toClientRowJson(session) + ","
                + "\"subscribedTopics\":[" + topicsJson + "]"
                + "}";
    }

    private String toClientRowJson(ClientSession session) {
        String clientIp = "unknown";
        if (session != null && session.channel() != null && session.channel().remoteAddress() instanceof InetSocketAddress address) {
            if (address.getAddress() != null) {
                clientIp = address.getAddress().getHostAddress();
            } else if (address.getHostString() != null) {
                clientIp = address.getHostString();
            }
        }
        long connectedAt = session == null || session.connectedAt() == null ? Instant.now().toEpochMilli() : session.connectedAt().toEpochMilli();
        return "{"
                + "\"clientId\":\"" + escape(session == null ? "" : session.clientId()) + "\","
                + "\"nodeId\":\"" + escape(nodeId) + "\","
                + "\"clientIp\":\"" + escape(clientIp) + "\","
                + "\"keepAliveSeconds\":" + (session == null ? 0 : session.keepAliveSeconds()) + ","
                + "\"connectionType\":\"" + escape(session == null ? "" : session.connectionType()) + "\","
                + "\"username\":\"" + escape(session == null ? "" : session.username()) + "\","
                + "\"connectedAt\":" + connectedAt
                + "}";
    }

    private static String toClustersJson(List<EmbeddedAdminStateStore.ClusterSummary> clusters) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < clusters.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(toClusterJson(clusters.get(i)));
        }
        builder.append("]");
        return builder.toString();
    }

    private static String toClusterJson(EmbeddedAdminStateStore.ClusterSummary cluster) {
        return "{"
                + "\"clusterId\":\"" + escape(cluster.clusterId()) + "\","
                + "\"displayName\":\"" + escape(cluster.displayName()) + "\","
                + "\"seedCoreNode\":\"" + escape(cluster.seedCoreNode()) + "\","
                + "\"createdAt\":" + cluster.createdAt()
                + "}";
    }

    private static String toClusterConfigJson(EmbeddedAdminStateStore.ClusterConfig config) {
        return "{"
                + "\"coreNodes\":" + toStringArray(config.coreNodes()) + ","
                + "\"replicantNodes\":" + toStringArray(config.replicantNodes()) + ","
                + "\"coreAcceptClientConnections\":" + config.coreAcceptClientConnections() + ","
                + "\"sharedSubscriptionMaxMembersPerGroup\":" + config.sharedSubscriptionMaxMembersPerGroup()
                + "}";
    }

    private static String toSecurityConfigJson(EmbeddedAdminStateStore.SecurityConfig config) {
        return "{"
                + "\"aclEnabled\":" + config.aclEnabled() + ","
                + "\"aclChain\":" + toStringArray(config.aclChain()) + ","
                + "\"authEnabled\":" + config.authEnabled() + ","
                + "\"authChain\":" + toStringArray(config.authChain()) + ","
                + "\"cacheTtlMs\":" + config.cacheTtlMs() + ","
                + "\"authHttp\":" + toAuthHttpJson(config.authHttp()) + ","
                + "\"authFile\":" + toAuthFileJson(config.authFile()) + ","
                + "\"authBuiltInDatabase\":" + toAuthBuiltInDatabaseJson(config.authBuiltInDatabase()) + ","
                + "\"authRedis\":" + toAuthRedisJson(config.authRedis()) + ","
                + "\"authMysql\":" + toAuthMysqlJson(config.authMysql()) + ","
                + "\"authPostgresql\":" + toAuthPostgresqlJson(config.authPostgresql())
                + "}";
    }

    private static String toAuthHttpJson(EmbeddedAdminStateStore.AuthHttpConfig config) {
        return "{"
                + "\"url\":\"" + escape(config.url()) + "\","
                + "\"timeoutMs\":" + config.timeoutMs()
                + "}";
    }

    private static String toAuthFileJson(EmbeddedAdminStateStore.AuthFileConfig config) {
        return "{"
                + "\"path\":\"" + escape(config.path()) + "\""
                + "}";
    }

    private static String toAuthBuiltInDatabaseJson(EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config) {
        return "{"
                + "\"accountType\":\"" + escape(config.accountType()) + "\","
                + "\"passwordHashAlgorithm\":\"" + escape(config.passwordHashAlgorithm()) + "\","
                + "\"saltPosition\":\"" + escape(config.saltPosition()) + "\""
                + "}";
    }

    private static String toAuthRedisJson(EmbeddedAdminStateStore.AuthRedisConfig config) {
        return "{"
                + "\"host\":\"" + escape(config.host()) + "\","
                + "\"port\":" + config.port() + ","
                + "\"password\":\"" + escape(config.password()) + "\","
                + "\"db\":" + config.db() + ","
                + "\"keyPrefix\":\"" + escape(config.keyPrefix()) + "\","
                + "\"timeoutMs\":" + config.timeoutMs()
                + "}";
    }

    private static String toAuthMysqlJson(EmbeddedAdminStateStore.AuthMysqlConfig config) {
        return "{"
                + "\"url\":\"" + escape(config.url()) + "\","
                + "\"user\":\"" + escape(config.user()) + "\","
                + "\"password\":\"" + escape(config.password()) + "\","
                + "\"query\":\"" + escape(config.query()) + "\","
                + "\"poolMinIdle\":" + config.poolMinIdle() + ","
                + "\"poolMaxSize\":" + config.poolMaxSize() + ","
                + "\"poolConnectionTimeoutMs\":" + config.poolConnectionTimeoutMs() + ","
                + "\"poolIdleTimeoutMs\":" + config.poolIdleTimeoutMs() + ","
                + "\"poolMaxLifetimeMs\":" + config.poolMaxLifetimeMs()
                + "}";
    }

    private static String toAuthPostgresqlJson(EmbeddedAdminStateStore.AuthPostgresqlConfig config) {
        return "{"
                + "\"url\":\"" + escape(config.url()) + "\","
                + "\"user\":\"" + escape(config.user()) + "\","
                + "\"password\":\"" + escape(config.password()) + "\","
                + "\"query\":\"" + escape(config.query()) + "\","
                + "\"poolMinIdle\":" + config.poolMinIdle() + ","
                + "\"poolMaxSize\":" + config.poolMaxSize() + ","
                + "\"poolConnectionTimeoutMs\":" + config.poolConnectionTimeoutMs() + ","
                + "\"poolIdleTimeoutMs\":" + config.poolIdleTimeoutMs() + ","
                + "\"poolMaxLifetimeMs\":" + config.poolMaxLifetimeMs()
                + "}";
    }

    private static String toFullConfigJson(EmbeddedAdminStateStore.ClusterFullConfig config) {
        return "{"
                + "\"summary\":" + toClusterJson(config.summary()) + ","
                + "\"clusterConfig\":" + toClusterConfigJson(config.clusterConfig()) + ","
                + "\"securityConfig\":" + toSecurityConfigJson(config.securityConfig())
                + "}";
    }

    private static String toNodeMetricsJson(EmbeddedAdminStateStore.NodeMetrics metrics) {
        return "{"
                + "\"nodeId\":\"" + escape(metrics.nodeId()) + "\","
                + "\"nodeIp\":\"" + escape(metrics.nodeIp()) + "\","
                + "\"role\":\"" + escape(metrics.role()) + "\","
                + "\"inboundBytes\":" + metrics.inboundBytes() + ","
                + "\"outboundBytes\":" + metrics.outboundBytes() + ","
                + "\"connectedClients\":" + metrics.connectedClients() + ","
                + "\"lastReportTime\":" + metrics.reportTime()
                + "}";
    }

    private static String toAuditLogJson(EmbeddedAdminStateStore.AuditLogEntry entry) {
        return "{"
                + "\"id\":\"" + escape(entry.id()) + "\","
                + "\"clusterId\":\"" + escape(entry.clusterId()) + "\","
                + "\"action\":\"" + escape(entry.action()) + "\","
                + "\"source\":\"" + escape(entry.source()) + "\","
                + "\"timestamp\":" + entry.timestamp() + ","
                + "\"beforeJson\":\"" + escape(entry.beforeJson()) + "\","
                + "\"afterJson\":\"" + escape(entry.afterJson()) + "\""
                + "}";
    }

    private static String toAuditLogsJson(List<EmbeddedAdminStateStore.AuditLogEntry> entries) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(toAuditLogJson(entries.get(i)));
        }
        builder.append("]");
        return builder.toString();
    }

    private String buildBuiltInUsersJson(String clusterId) {
        EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config = stateStore.getSecurityConfig(clusterId).authBuiltInDatabase();
        List<BuiltInDatabaseUserService.UserRecord> users = builtInDatabaseUserService.listUsers();
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < users.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            BuiltInDatabaseUserService.UserRecord record = users.get(i);
            builder.append("{")
                    .append("\"userId\":\"").append(escape(record.userId())).append("\",")
                    .append("\"salted\":").append(record.salted()).append(",")
                    .append("\"iterations\":").append(record.iterations()).append(",")
                    .append("\"superuser\":").append(record.superuser())
                    .append("}");
        }
        builder.append("]");
        return "{"
                + "\"accountType\":\"" + escape(config.accountType()) + "\","
                + "\"passwordHashAlgorithm\":\"" + escape(config.passwordHashAlgorithm()) + "\","
                + "\"saltPosition\":\"" + escape(config.saltPosition()) + "\","
                + "\"records\":" + builder
                + "}";
    }

    private static String toStringArray(List<String> values) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append("\"").append(escape(values.get(i))).append("\"");
        }
        builder.append("]");
        return builder.toString();
    }

    private static EmbeddedAdminStateStore.ClusterConfig parseClusterConfig(String body, EmbeddedAdminStateStore.ClusterConfig current) {
        List<String> coreNodes = extractStringList(body, "coreNodes");
        List<String> replicantNodes = extractStringList(body, "replicantNodes");
        boolean acceptClients = extractBoolean(body, "coreAcceptClientConnections", current.coreAcceptClientConnections());
        int sharedMax = extractInt(body, "sharedSubscriptionMaxMembersPerGroup", current.sharedSubscriptionMaxMembersPerGroup());
        if (coreNodes.isEmpty()) {
            coreNodes = current.coreNodes();
        }
        return new EmbeddedAdminStateStore.ClusterConfig(coreNodes, replicantNodes, acceptClients, sharedMax);
    }

    private static EmbeddedAdminStateStore.SecurityConfig parseSecurityConfig(String body, EmbeddedAdminStateStore.SecurityConfig current) {
        boolean aclEnabled = extractBoolean(body, "aclEnabled", current.aclEnabled());
        boolean authEnabled = extractBoolean(body, "authEnabled", current.authEnabled());
        List<String> aclChain = extractStringList(body, "aclChain");
        List<String> authChain = extractStringList(body, "authChain");
        long cacheTtlMs = extractLong(body, "cacheTtlMs", current.cacheTtlMs());
        if (aclChain.isEmpty()) {
            aclChain = current.aclChain();
        }
        if (authChain.isEmpty()) {
            authChain = current.authChain();
        }
        EmbeddedAdminStateStore.AuthHttpConfig authHttp = parseAuthHttpConfig(body, current.authHttp());
        EmbeddedAdminStateStore.AuthFileConfig authFile = parseAuthFileConfig(body, current.authFile());
        EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig authBuiltInDatabase = parseAuthBuiltInDatabaseConfig(body, current.authBuiltInDatabase());
        EmbeddedAdminStateStore.AuthRedisConfig authRedis = parseAuthRedisConfig(body, current.authRedis());
        EmbeddedAdminStateStore.AuthMysqlConfig authMysql = parseAuthMysqlConfig(body, current.authMysql());
        EmbeddedAdminStateStore.AuthPostgresqlConfig authPostgresql = parseAuthPostgresqlConfig(body, current.authPostgresql());
        return new EmbeddedAdminStateStore.SecurityConfig(
                aclEnabled,
                aclChain,
                authEnabled,
                authChain,
                cacheTtlMs,
                authHttp,
                authFile,
                authBuiltInDatabase,
                authRedis,
                authMysql,
                authPostgresql
        );
    }

    private EmbeddedAdminStateStore.NodeMetrics parseNodeMetrics(String reportedNodeId, String body, String clusterId) {
        String effectiveNodeId = normalize(reportedNodeId, nodeId);
        String effectiveNodeIp = normalize(extractString(body, "nodeIp"), "unknown");
        long inbound = Math.max(0L, extractLong(body, "inboundBytes", 0L));
        long outbound = Math.max(0L, extractLong(body, "outboundBytes", 0L));
        int connections = Math.max(0, extractInt(body, "connectedClients", 0));
        long reportTime = extractLong(body, "reportTime", System.currentTimeMillis());

        String role = "";
        for (EmbeddedAdminStateStore.NodeMetrics current : stateStore.listNodeMetrics(clusterId)) {
            if (effectiveNodeId.equals(current.nodeId())) {
                role = current.role();
                break;
            }
        }
        if (effectiveNodeId.equals(nodeId) && (role == null || role.isBlank())) {
            role = nodeRole;
        }
        if (role == null || role.isBlank()) {
            role = "UNKNOWN";
        }
        return new EmbeddedAdminStateStore.NodeMetrics(
                effectiveNodeId,
                effectiveNodeIp,
                role,
                inbound,
                outbound,
                connections,
                reportTime
        );
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> query = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            return query;
        }
        String[] pairs = raw.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            if (idx < 0) {
                query.put(decode(pair), "");
                continue;
            }
            String key = decode(pair.substring(0, idx));
            String value = decode(pair.substring(idx + 1));
            query.put(key, value);
        }
        return query;
    }

    private static String decode(String value) {
        return URLDecoder.decode(value == null ? "" : value, StandardCharsets.UTF_8);
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (Exception ignored) {
            return defaultValue;
        }
    }

    private static boolean containsIgnoreCase(String source, String target) {
        if (target == null || target.isBlank()) {
            return true;
        }
        if (source == null) {
            return false;
        }
        return source.toLowerCase().contains(target.toLowerCase());
    }

    private static String normalize(String value, String defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static String extractString(String body, String key) {
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]*)\"").matcher(body);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private static List<String> extractStringList(String body, String key) {
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\\[(.*?)\\]", Pattern.DOTALL).matcher(body);
        if (!matcher.find()) {
            return List.of();
        }
        String content = matcher.group(1).trim();
        if (content.isBlank()) {
            return List.of();
        }
        String[] parts = content.split(",");
        List<String> result = new ArrayList<>();
        for (String part : parts) {
            String value = part.trim();
            if (value.startsWith("\"")) {
                value = value.substring(1);
            }
            if (value.endsWith("\"")) {
                value = value.substring(0, value.length() - 1);
            }
            if (!value.isBlank()) {
                result.add(value);
            }
        }
        return result;
    }

    private static boolean extractBoolean(String body, String key, boolean defaultValue) {
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(true|false)").matcher(body);
        if (matcher.find()) {
            return Boolean.parseBoolean(matcher.group(1));
        }
        return defaultValue;
    }

    private static int extractInt(String body, String key, int defaultValue) {
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(-?\\d+)").matcher(body);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (Exception ignored) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private static long extractLong(String body, String key, long defaultValue) {
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(-?\\d+)").matcher(body);
        if (matcher.find()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (Exception ignored) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private static EmbeddedAdminStateStore.AuthHttpConfig parseAuthHttpConfig(
            String body,
            EmbeddedAdminStateStore.AuthHttpConfig current
    ) {
        String segment = extractObject(body, "authHttp");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AuthHttpConfig(
                normalize(extractString(segment, "url"), current.url()),
                extractInt(segment, "timeoutMs", current.timeoutMs())
        );
    }

    private static EmbeddedAdminStateStore.AuthFileConfig parseAuthFileConfig(
            String body,
            EmbeddedAdminStateStore.AuthFileConfig current
    ) {
        String segment = extractObject(body, "authFile");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AuthFileConfig(
                normalize(extractString(segment, "path"), current.path())
        );
    }

    private static EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig parseAuthBuiltInDatabaseConfig(
            String body,
            EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig current
    ) {
        String segment = extractObject(body, "authBuiltInDatabase");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                normalize(extractString(segment, "accountType"), current.accountType()),
                normalize(extractString(segment, "passwordHashAlgorithm"), current.passwordHashAlgorithm()),
                normalize(extractString(segment, "saltPosition"), current.saltPosition())
        );
    }

    private static EmbeddedAdminStateStore.AuthRedisConfig parseAuthRedisConfig(
            String body,
            EmbeddedAdminStateStore.AuthRedisConfig current
    ) {
        String segment = extractObject(body, "authRedis");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AuthRedisConfig(
                normalize(extractString(segment, "host"), current.host()),
                extractInt(segment, "port", current.port()),
                extractString(segment, "password") == null ? current.password() : extractString(segment, "password"),
                extractInt(segment, "db", current.db()),
                normalize(extractString(segment, "keyPrefix"), current.keyPrefix()),
                extractInt(segment, "timeoutMs", current.timeoutMs())
        );
    }

    private static EmbeddedAdminStateStore.AuthMysqlConfig parseAuthMysqlConfig(
            String body,
            EmbeddedAdminStateStore.AuthMysqlConfig current
    ) {
        String segment = extractObject(body, "authMysql");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AuthMysqlConfig(
                normalize(extractString(segment, "url"), current.url()),
                normalize(extractString(segment, "user"), current.user()),
                extractString(segment, "password") == null ? current.password() : extractString(segment, "password"),
                normalize(extractString(segment, "query"), current.query()),
                extractInt(segment, "poolMinIdle", current.poolMinIdle()),
                extractInt(segment, "poolMaxSize", current.poolMaxSize()),
                extractLong(segment, "poolConnectionTimeoutMs", current.poolConnectionTimeoutMs()),
                extractLong(segment, "poolIdleTimeoutMs", current.poolIdleTimeoutMs()),
                extractLong(segment, "poolMaxLifetimeMs", current.poolMaxLifetimeMs())
        );
    }

    private static EmbeddedAdminStateStore.AuthPostgresqlConfig parseAuthPostgresqlConfig(
            String body,
            EmbeddedAdminStateStore.AuthPostgresqlConfig current
    ) {
        String segment = extractObject(body, "authPostgresql");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                normalize(extractString(segment, "url"), current.url()),
                normalize(extractString(segment, "user"), current.user()),
                extractString(segment, "password") == null ? current.password() : extractString(segment, "password"),
                normalize(extractString(segment, "query"), current.query()),
                extractInt(segment, "poolMinIdle", current.poolMinIdle()),
                extractInt(segment, "poolMaxSize", current.poolMaxSize()),
                extractLong(segment, "poolConnectionTimeoutMs", current.poolConnectionTimeoutMs()),
                extractLong(segment, "poolIdleTimeoutMs", current.poolIdleTimeoutMs()),
                extractLong(segment, "poolMaxLifetimeMs", current.poolMaxLifetimeMs())
        );
    }

    private static String validateSecurityConfig(EmbeddedAdminStateStore.SecurityConfig config) {
        if (config == null || !config.authEnabled() || config.authChain().isEmpty()) {
            return null;
        }
        for (String rawPlugin : config.authChain()) {
            String plugin = normalize(rawPlugin, "").toLowerCase();
            if ("mysql".equals(plugin)) {
                String error = validateJdbcConfig(
                        "MySQL",
                        "com.mysql.cj.jdbc.Driver",
                        config.authMysql().url(),
                        config.authMysql().user(),
                        config.authMysql().password(),
                        config.authMysql().query()
                );
                if (error != null) {
                    return error;
                }
            }
            if ("postgresql".equals(plugin)) {
                String error = validateJdbcConfig(
                        "PostgreSQL",
                        "org.postgresql.Driver",
                        config.authPostgresql().url(),
                        config.authPostgresql().user(),
                        config.authPostgresql().password(),
                        config.authPostgresql().query()
                );
                if (error != null) {
                    return error;
                }
            }
        }
        return null;
    }

    private static String validateJdbcConfig(
            String databaseType,
            String driverClassName,
            String url,
            String user,
            String password,
            String query
    ) {
        try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException exception) {
            return databaseType + " 驱动未找到";
        }
        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement ignored = connection.prepareStatement(query)) {
            return null;
        } catch (Exception exception) {
            return databaseType + " 连接验证失败: " + normalize(exception.getMessage(), exception.getClass().getSimpleName());
        }
    }

    private static String extractObject(String body, String key) {
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\\{", Pattern.DOTALL).matcher(body);
        if (!matcher.find()) {
            return null;
        }
        int start = matcher.end() - 1;
        int depth = 0;
        for (int i = start; i < body.length(); i++) {
            char current = body.charAt(i);
            if (current == '{') {
                depth++;
            } else if (current == '}') {
                depth--;
                if (depth == 0) {
                    return body.substring(start, i + 1);
                }
            }
        }
        return null;
    }

    private static void writeJson(HttpExchange exchange, int code, String json) throws IOException {
        write(exchange, code, "application/json; charset=UTF-8", json.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] readAll(InputStream inputStream) throws IOException {
        try (InputStream in = inputStream; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read > 0) {
                    out.write(buffer, 0, read);
                }
            }
            return out.toByteArray();
        }
    }

    private static String normalizeBasePath(String value) {
        if (value == null || value.isBlank()) {
            return "/admin";
        }
        String normalized = value.trim();
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized.isBlank() ? "/admin" : normalized;
    }

    private static String stripTrailingSlash(String value) {
        if (value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private void appendAuditLog(String clusterId, String action, String source, String beforeJson, String afterJson) {
        long now = System.currentTimeMillis();
        stateStore.appendAuditLog(clusterId, new EmbeddedAdminStateStore.AuditLogEntry(
                clusterId + "-" + now,
                clusterId,
                action,
                source,
                now,
                beforeJson,
                afterJson
        ));
    }

    private static String resolveAuditSource(HttpExchange exchange) {
        if (exchange == null || exchange.getRemoteAddress() == null) {
            return "unknown";
        }
        if (exchange.getRemoteAddress().getAddress() != null) {
            return exchange.getRemoteAddress().getAddress().getHostAddress();
        }
        return String.valueOf(exchange.getRemoteAddress());
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }

    @FunctionalInterface
    public interface SecurityConfigUpdater {
        void apply(String clusterId, EmbeddedAdminStateStore.SecurityConfig config);
    }

    @FunctionalInterface
    public interface ClusterConfigUpdater {
        void apply(String clusterId, EmbeddedAdminStateStore.ClusterConfig config);
    }

    public interface BuiltInDatabaseUsersUpdater {
        default void upsert(String clusterId, EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config, String userId, String password, boolean superuser) {
        }

        default int importUsers(String clusterId, EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config, List<BuiltInDatabaseUserService.UserInput> users) {
            return 0;
        }

        default void delete(String clusterId, String userId) {
        }

        default void deleteAll(String clusterId) {
        }
    }
}
