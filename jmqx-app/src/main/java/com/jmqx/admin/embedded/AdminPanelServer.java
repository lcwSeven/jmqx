package com.jmqx.admin.embedded;

import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.session.ClientSession;
import com.jmqx.session.SessionRegistry;
import com.jmqx.transport.ConnectionMetrics;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
    private static final MediaType OCTET_STREAM_MEDIA_TYPE = MediaType.get("application/octet-stream");

    private final String host;
    private final int port;
    private final String basePath;
    private final String backendBaseUrl;
    private final AdminAuthRuntime adminAuthRuntime;
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
    private final BridgeConfigUpdater bridgeConfigUpdater;
    private final ClientKickUpdater clientKickUpdater;
    private final BlacklistUpdater blacklistUpdater;
    private final BuiltInDatabaseUsersUpdater builtInDatabaseUsersUpdater;
    private final OkHttpClient httpClient;
    private HttpServer server;

    public AdminPanelServer(String host,
                            int port,
                            String basePath,
                            String backendBaseUrl,
                            AdminAuthRuntime adminAuthRuntime,
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
                            EmbeddedAdminStateStore.BridgeConfig initialBridgeConfig,
                            SecurityConfigUpdater securityConfigUpdater,
                            ClusterConfigUpdater clusterConfigUpdater,
                            BridgeConfigUpdater bridgeConfigUpdater,
                            ClientKickUpdater clientKickUpdater,
                            BlacklistUpdater blacklistUpdater,
                            BuiltInDatabaseUsersUpdater builtInDatabaseUsersUpdater,
                            BuiltInDatabaseUserService builtInDatabaseUserService) {
        this.host = (host == null || host.isBlank()) ? "0.0.0.0" : host.trim();
        this.port = port <= 0 ? 18081 : port;
        this.basePath = normalizeBasePath(basePath);
        this.backendBaseUrl = stripTrailingSlash(backendBaseUrl == null || backendBaseUrl.isBlank()
                ? "http://127.0.0.1:18080"
                : backendBaseUrl.trim());
        this.adminAuthRuntime = adminAuthRuntime == null ? new AdminAuthRuntime(AdminAuthRuntime.Config.defaults()) : adminAuthRuntime;
        this.defaultClusterId = normalize(defaultClusterId, "default");
        this.nodeId = normalize(nodeId, "node-1");
        this.nodeIp = normalize(nodeIp, "unknown");
        this.nodeRole = normalize(nodeRole, "CORE");
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.connectionMetrics = connectionMetrics;
        this.stateStore = stateStore == null ? new EmbeddedAdminStateStore() : stateStore;
        this.builtInDatabaseUserService = builtInDatabaseUserService == null ? new BuiltInDatabaseUserService() : builtInDatabaseUserService;
        if (!this.stateStore.hasAdminAuthConfig()) {
            this.stateStore.setAdminAuthConfig(this.adminAuthRuntime.current());
        } else {
            this.adminAuthRuntime.update(this.stateStore.getAdminAuthConfig());
        }
        this.stateStore.createCluster(this.defaultClusterId, "默认集群", this.nodeIp + ":7800");
        if (initialSecurityConfig != null && !this.stateStore.hasSecurityConfig(this.defaultClusterId)) {
            this.stateStore.setSecurityConfig(this.defaultClusterId, initialSecurityConfig);
        }
        if (initialClusterConfig != null && !this.stateStore.hasClusterConfig(this.defaultClusterId)) {
            this.stateStore.setClusterConfig(this.defaultClusterId, initialClusterConfig);
        }
        if (initialBridgeConfig != null && !this.stateStore.hasBridgeConfig(this.defaultClusterId)) {
            this.stateStore.setBridgeConfig(this.defaultClusterId, initialBridgeConfig);
        }
        this.securityConfigUpdater = securityConfigUpdater == null ? (clusterId, config) -> {
        } : securityConfigUpdater;
        this.clusterConfigUpdater = clusterConfigUpdater == null ? (clusterId, config) -> {
        } : clusterConfigUpdater;
        this.bridgeConfigUpdater = bridgeConfigUpdater == null ? (clusterId, config) -> {
        } : bridgeConfigUpdater;
        this.clientKickUpdater = clientKickUpdater == null ? (clusterId, clientId) -> {
        } : clientKickUpdater;
        this.blacklistUpdater = blacklistUpdater == null ? new BlacklistUpdater() {
        } : blacklistUpdater;
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
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(2))
                .callTimeout(Duration.ofSeconds(5))
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
        boolean internalApi = path.startsWith("/api/v1/internal/");
        AdminPrincipal adminPrincipal = internalApi ? new AdminPrincipal("internal", "super_admin") : resolveAuthorizedPrincipal(exchange);
        if (!internalApi && isAuthRequired() && adminPrincipal == null) {
            exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"JMQX Admin\"");
            writeJson(exchange, 401, "{\"error\":\"unauthorized\"}");
            return;
        }
        String method = exchange.getRequestMethod().toUpperCase();
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        String clusterId = normalize(query.get("clusterId"), defaultClusterId);
        byte[] requestBody = readAll(exchange.getRequestBody());
        String body = new String(requestBody, StandardCharsets.UTF_8);

        if ("/api/v1/admin/session".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, toAdminSessionJson(adminPrincipal));
            return;
        }
        if ("/api/v1/admin/password".equals(path) && "PUT".equals(method)) {
            String currentPassword = normalize(extractString(body, "currentPassword"), "");
            String newPassword = normalize(extractString(body, "newPassword"), "");
            String confirmPassword = normalize(extractString(body, "confirmPassword"), newPassword);
            if (!adminAuthRuntime.matches(adminPrincipal.username(), currentPassword)) {
                writeJson(exchange, 400, "{\"error\":\"当前密码不正确\"}");
                return;
            }
            if (newPassword.length() < 4) {
                writeJson(exchange, 400, "{\"error\":\"新密码长度至少为 4 位\"}");
                return;
            }
            if (!newPassword.equals(confirmPassword)) {
                writeJson(exchange, 400, "{\"error\":\"两次输入的新密码不一致\"}");
                return;
            }
            AdminAuthRuntime.Config before = adminAuthRuntime.current();
            AdminAuthRuntime.Config updated = new AdminAuthRuntime.Config(
                    before.username(),
                    newPassword,
                    before.role()
            ).normalize();
            adminAuthRuntime.update(updated);
            stateStore.setAdminAuthConfig(updated);
            appendAuditLog(
                    clusterId,
                    "admin.password.updated",
                    resolveAdminSource(exchange, adminPrincipal),
                    toAdminAuditJson(before),
                    toAdminAuditJson(updated)
            );
            writeJson(exchange, 200, "{\"success\":true}");
            return;
        }
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
        if ("/api/v1/internal/clients".equals(path) && "POST".equals(method)) {
            EmbeddedAdminStateStore.ClientSnapshot clientSnapshot = parseClientSnapshot(body);
            stateStore.upsertClientSnapshot(clusterId, clientSnapshot);
            writeJson(exchange, 200, "{\"ok\":true}");
            return;
        }
        if (path.startsWith("/api/v1/internal/clients/") && path.endsWith("/subscriptions") && "POST".equals(method)) {
            String clientId = decode(path.substring("/api/v1/internal/clients/".length(), path.length() - "/subscriptions".length()));
            stateStore.replaceClientSubscriptions(clusterId, clientId, extractStringList(body, "topics"));
            writeJson(exchange, 200, "{\"ok\":true}");
            return;
        }
        if (path.startsWith("/api/v1/internal/clients/") && "DELETE".equals(method)) {
            String clientId = decode(path.substring("/api/v1/internal/clients/".length()));
            stateStore.removeClientSnapshot(clusterId, clientId);
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
        if ("/api/v1/bridge/config".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, toBridgeConfigJson(stateStore.getBridgeConfig(clusterId)));
            return;
        }
        if ("/api/v1/bridge/config".equals(path) && "PUT".equals(method)) {
            EmbeddedAdminStateStore.BridgeConfig before = stateStore.getBridgeConfig(clusterId);
            EmbeddedAdminStateStore.BridgeConfig config = parseBridgeConfig(body, before);
            String validationError = validateBridgeConfig(config);
            if (validationError != null) {
                writeJson(exchange, 400, "{\"error\":\"" + escape(validationError) + "\"}");
                return;
            }
            bridgeConfigUpdater.apply(clusterId, config);
            stateStore.setBridgeConfig(clusterId, config);
            appendAuditLog(
                    clusterId,
                    "bridge.config.updated",
                    resolveAuditSource(exchange),
                    toBridgeConfigJson(before),
                    toBridgeConfigJson(config)
            );
            writeJson(exchange, 200, toBridgeConfigJson(config));
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
        if ("/api/v1/blacklist".equals(path) && "GET".equals(method)) {
            writeJson(exchange, 200, buildBlacklistJson(clusterId));
            return;
        }
        if ("/api/v1/blacklist".equals(path) && "POST".equals(method)) {
            EmbeddedAdminStateStore.BlacklistEntry entry = parseBlacklistEntry(body, resolveAdminSource(exchange, adminPrincipal));
            if (entry == null || entry.value().isBlank()) {
                writeJson(exchange, 400, "{\"error\":\"invalid blacklist entry\"}");
                return;
            }
            blacklistUpdater.upsert(clusterId, entry);
            stateStore.upsertBlacklistEntry(clusterId, entry);
            appendAuditLog(
                    clusterId,
                    "security.blacklist.upserted",
                    resolveAdminSource(exchange, adminPrincipal),
                    "{}",
                    toBlacklistEntryJson(entry)
            );
            writeJson(exchange, 200, buildBlacklistJson(clusterId));
            return;
        }
        if (path.startsWith("/api/v1/blacklist/") && "DELETE".equals(method)) {
            String suffix = decode(path.substring("/api/v1/blacklist/".length()));
            int split = suffix.indexOf('/');
            if (split <= 0) {
                writeJson(exchange, 400, "{\"error\":\"invalid blacklist path\"}");
                return;
            }
            String type = suffix.substring(0, split);
            String value = suffix.substring(split + 1);
            blacklistUpdater.delete(clusterId, type, value);
            stateStore.removeBlacklistEntry(clusterId, type, value);
            appendAuditLog(
                    clusterId,
                    "security.blacklist.deleted",
                    resolveAdminSource(exchange, adminPrincipal),
                    "{\"type\":\"" + escape(type) + "\",\"value\":\"" + escape(value) + "\"}",
                    "{}"
            );
            writeJson(exchange, 200, buildBlacklistJson(clusterId));
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
        if (path.startsWith("/api/v1/clients/") && path.endsWith("/kick") && "POST".equals(method)) {
            String clientId = decode(path.substring("/api/v1/clients/".length(), path.length() - "/kick".length()));
            if (clientId == null || clientId.isBlank()) {
                writeJson(exchange, 400, "{\"error\":\"clientId is required\"}");
                return;
            }
            clientKickUpdater.apply(clusterId, clientId);
            appendAuditLog(
                    clusterId,
                    "client.kicked",
                    resolveAdminSource(exchange, adminPrincipal),
                    "{}",
                    "{\"clientId\":\"" + escape(clientId) + "\"}"
            );
            writeJson(exchange, 200, "{\"success\":true}");
            return;
        }
        if (path.startsWith("/api/v1/clients/") && "GET".equals(method)) {
            String clientId = decode(path.substring("/api/v1/clients/".length()));
            String detailJson = buildClientDetailJson(clusterId, clientId);
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
        Request.Builder builder = new Request.Builder()
                .url(URI.create(target).toString());

        String method = exchange.getRequestMethod();
        if ("GET".equalsIgnoreCase(method)) {
            builder.get();
        } else {
            builder.method(method, RequestBody.create(requestBody, OCTET_STREAM_MEDIA_TYPE));
        }

        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType != null && !contentType.isBlank()) {
            builder.header("Content-Type", contentType);
        }

        try {
            try (Response response = httpClient.newCall(builder.build()).execute()) {
                Headers headers = exchange.getResponseHeaders();
                for (String name : response.headers().names()) {
                    if ("transfer-encoding".equalsIgnoreCase(name)) {
                        continue;
                    }
                    headers.put(name, response.headers(name));
                }
                byte[] responseBody = response.body() == null ? new byte[0] : response.body().bytes();
                write(exchange, response.code(), headers.getFirst("Content-Type"), responseBody);
            }
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
        int connections = sessionRegistry == null ? 0 : sessionRegistry.list().size();
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
        String clusterId = normalize(query.get("clusterId"), defaultClusterId);
        String clientIdFilter = normalize(query.get("clientId"), "");
        String userNameFilter = normalize(query.get("userName"), "");
        int pageNo = parseInt(query.get("pageNo"), 1);
        int pageSize = Math.min(Math.max(parseInt(query.get("pageSize"), 20), 1), 200);
        upsertLocalClientSnapshots(clusterId);
        List<EmbeddedAdminStateStore.ClientSnapshot> snapshots = stateStore.listClientSnapshots(clusterId);
        List<EmbeddedAdminStateStore.ClientSnapshot> filtered = new ArrayList<>();
        for (EmbeddedAdminStateStore.ClientSnapshot snapshot : snapshots) {
            if (snapshot == null) {
                continue;
            }
            if (!containsIgnoreCase(snapshot.clientId(), clientIdFilter)) {
                continue;
            }
            if (!containsIgnoreCase(snapshot.username(), userNameFilter)) {
                continue;
            }
            filtered.add(snapshot);
        }
        filtered.sort(Comparator.comparing(EmbeddedAdminStateStore.ClientSnapshot::connectedAt).reversed());
        int from = Math.max((pageNo - 1) * pageSize, 0);
        int to = Math.min(from + pageSize, filtered.size());
        List<EmbeddedAdminStateStore.ClientSnapshot> page = from >= filtered.size() ? List.of() : filtered.subList(from, to);

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

    private String buildClientDetailJson(String clusterId, String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return null;
        }
        upsertLocalClientSnapshots(clusterId);
        EmbeddedAdminStateStore.ClientSnapshot snapshot = stateStore.getClientSnapshot(clusterId, clientId);
        if (snapshot == null) {
            return null;
        }
        Set<String> topics = new LinkedHashSet<>(snapshot.subscribedTopics());
        StringBuilder topicsJson = new StringBuilder();
        int i = 0;
        for (String topic : topics) {
            if (i++ > 0) {
                topicsJson.append(",");
            }
            topicsJson.append("\"").append(escape(topic)).append("\"");
        }
        return "{"
                + "\"session\":" + toClientRowJson(snapshot) + ","
                + "\"subscribedTopics\":[" + topicsJson + "]"
                + "}";
    }

    private void upsertLocalClientSnapshots(String clusterId) {
        if (sessionRegistry == null) {
            return;
        }
        Set<String> activeClientIds = new LinkedHashSet<>();
        for (ClientSession session : sessionRegistry.list()) {
            if (session == null) {
                continue;
            }
            activeClientIds.add(session.clientId());
            Set<String> topics = subscriptionRegistry == null
                    ? Set.of()
                    : new LinkedHashSet<>(subscriptionRegistry.findSubscriptions(session.clientId()).keySet());
            stateStore.upsertClientSnapshot(clusterId, new EmbeddedAdminStateStore.ClientSnapshot(
                    session.clientId(),
                    nodeId,
                    resolveClientIp(session),
                    session.keepAliveSeconds(),
                    session.connectionType(),
                    session.username(),
                    session.connectedAt() == null ? 0L : session.connectedAt().toEpochMilli(),
                    new ArrayList<>(topics)
            ));
        }
        for (EmbeddedAdminStateStore.ClientSnapshot snapshot : stateStore.listClientSnapshots(clusterId)) {
            if (snapshot == null || !nodeId.equals(snapshot.nodeId())) {
                continue;
            }
            if (!activeClientIds.contains(snapshot.clientId())) {
                stateStore.removeClientSnapshot(clusterId, snapshot.clientId());
            }
        }
    }

    private String toClientRowJson(EmbeddedAdminStateStore.ClientSnapshot snapshot) {
        return "{"
                + "\"clientId\":\"" + escape(snapshot == null ? "" : snapshot.clientId()) + "\","
                + "\"nodeId\":\"" + escape(snapshot == null ? "" : snapshot.nodeId()) + "\","
                + "\"clientIp\":\"" + escape(snapshot == null ? "" : snapshot.clientIp()) + "\","
                + "\"keepAliveSeconds\":" + (snapshot == null ? 0 : snapshot.keepAliveSeconds()) + ","
                + "\"connectionType\":\"" + escape(snapshot == null ? "" : snapshot.connectionType()) + "\","
                + "\"username\":\"" + escape(snapshot == null ? "" : snapshot.username()) + "\","
                + "\"connectedAt\":" + (snapshot == null ? 0L : snapshot.connectedAt())
                + "}";
    }

    private static String resolveClientIp(ClientSession session) {
        String clientIp = "unknown";
        if (session != null && session.channel() != null && session.channel().remoteAddress() instanceof InetSocketAddress address) {
            if (address.getAddress() != null) {
                clientIp = address.getAddress().getHostAddress();
            } else if (address.getHostString() != null) {
                clientIp = address.getHostString();
            }
        }
        return clientIp;
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
                + "\"aclDefaultAllow\":" + config.aclDefaultAllow() + ","
                + "\"aclHttp\":" + toAclHttpJson(config.aclHttp()) + ","
                + "\"aclFile\":" + toAclFileJson(config.aclFile()) + ","
                + "\"aclRedis\":" + toAclRedisJson(config.aclRedis()) + ","
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

    private static String toBridgeConfigJson(EmbeddedAdminStateStore.BridgeConfig config) {
        return "{"
                + "\"enabled\":" + config.enabled() + ","
                + "\"types\":" + toStringArray(config.types()) + ","
                + "\"topicFilters\":" + toStringArray(config.topicFilters()) + ","
                + "\"asyncEnabled\":" + config.asyncEnabled() + ","
                + "\"asyncQueueCapacity\":" + config.asyncQueueCapacity() + ","
                + "\"asyncWorkerCount\":" + config.asyncWorkerCount() + ","
                + "\"kafka\":" + toBridgeKafkaJson(config.kafka()) + ","
                + "\"rocketmq\":" + toBridgeRocketmqJson(config.rocketmq()) + ","
                + "\"mysql\":" + toBridgeMysqlJson(config.mysql())
                + "}";
    }

    private static String toBridgeKafkaJson(EmbeddedAdminStateStore.BridgeKafkaConfig config) {
        return "{"
                + "\"enabled\":" + config.enabled() + ","
                + "\"bootstrapServers\":\"" + escape(config.bootstrapServers()) + "\","
                + "\"topic\":\"" + escape(config.topic()) + "\","
                + "\"sourceTopicFilters\":" + toStringArray(config.sourceTopicFilters()) + ","
                + "\"acks\":\"" + escape(config.acks()) + "\","
                + "\"clientId\":\"" + escape(config.clientId()) + "\","
                + "\"compressionType\":\"" + escape(config.compressionType()) + "\""
                + "}";
    }

    private static String toBridgeRocketmqJson(EmbeddedAdminStateStore.BridgeRocketmqConfig config) {
        return "{"
                + "\"enabled\":" + config.enabled() + ","
                + "\"nameServer\":\"" + escape(config.nameServer()) + "\","
                + "\"producerGroup\":\"" + escape(config.producerGroup()) + "\","
                + "\"topic\":\"" + escape(config.topic()) + "\","
                + "\"sourceTopicFilters\":" + toStringArray(config.sourceTopicFilters()) + ","
                + "\"syncSend\":" + config.syncSend() + ","
                + "\"timeoutMs\":" + config.timeoutMs()
                + "}";
    }

    private static String toBridgeMysqlJson(EmbeddedAdminStateStore.BridgeMysqlConfig config) {
        return "{"
                + "\"enabled\":" + config.enabled() + ","
                + "\"driver\":\"" + escape(config.driver()) + "\","
                + "\"url\":\"" + escape(config.url()) + "\","
                + "\"user\":\"" + escape(config.user()) + "\","
                + "\"password\":\"" + escape(config.password()) + "\","
                + "\"table\":\"" + escape(config.table()) + "\","
                + "\"sourceTopicFilters\":" + toStringArray(config.sourceTopicFilters()) + ","
                + "\"autoCreateTable\":" + config.autoCreateTable()
                + "}";
    }

    private static String toAclHttpJson(EmbeddedAdminStateStore.AclHttpConfig config) {
        return "{"
                + "\"url\":\"" + escape(config.url()) + "\","
                + "\"timeoutMs\":" + config.timeoutMs() + ","
                + "\"bodyTemplate\":\"" + escape(config.bodyTemplate()) + "\""
                + "}";
    }

    private static String toAclFileJson(EmbeddedAdminStateStore.AclFileConfig config) {
        return "{"
                + "\"path\":\"" + escape(config.path()) + "\""
                + "}";
    }

    private static String toAclRedisJson(EmbeddedAdminStateStore.AclRedisConfig config) {
        return "{"
                + "\"host\":\"" + escape(config.host()) + "\","
                + "\"port\":" + config.port() + ","
                + "\"password\":\"" + escape(config.password()) + "\","
                + "\"db\":" + config.db() + ","
                + "\"keyPrefix\":\"" + escape(config.keyPrefix()) + "\","
                + "\"timeoutMs\":" + config.timeoutMs()
                + "}";
    }

    private static String toAuthHttpJson(EmbeddedAdminStateStore.AuthHttpConfig config) {
        return "{"
                + "\"method\":\"" + escape(config.method()) + "\","
                + "\"url\":\"" + escape(config.url()) + "\","
                + "\"headersText\":\"" + escape(config.headersText()) + "\","
                + "\"tlsEnabled\":" + config.tlsEnabled() + ","
                + "\"bodyTemplate\":\"" + escape(config.bodyTemplate()) + "\","
                + "\"poolSize\":" + config.poolSize() + ","
                + "\"rateLimitPerSecond\":" + config.rateLimitPerSecond() + ","
                + "\"requestTimeoutMs\":" + config.requestTimeoutMs() + ","
                + "\"connectTimeoutMs\":" + config.connectTimeoutMs() + ","
                + "\"pipelineCount\":" + config.pipelineCount()
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

    private static String toBlacklistEntryJson(EmbeddedAdminStateStore.BlacklistEntry entry) {
        return "{"
                + "\"type\":\"" + escape(entry.type()) + "\","
                + "\"value\":\"" + escape(entry.value()) + "\","
                + "\"createdAt\":" + entry.createdAt() + ","
                + "\"source\":\"" + escape(entry.source()) + "\""
                + "}";
    }

    private static String toAdminSessionJson(AdminPrincipal principal) {
        if (principal == null) {
            return "{\"authenticated\":false}";
        }
        String permissions = principal.superAdmin()
                ? "[\"*\"]"
                : "[]";
        return "{"
                + "\"authenticated\":true,"
                + "\"username\":\"" + escape(principal.username()) + "\","
                + "\"role\":\"" + escape(principal.role()) + "\","
                + "\"superAdmin\":" + principal.superAdmin() + ","
                + "\"permissions\":" + permissions
                + "}";
    }

    private static String toAdminAuditJson(AdminAuthRuntime.Config config) {
        if (config == null) {
            return "{}";
        }
        return "{"
                + "\"username\":\"" + escape(config.username()) + "\","
                + "\"role\":\"" + escape(config.role()) + "\""
                + "}";
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

    private String buildBlacklistJson(String clusterId) {
        List<EmbeddedAdminStateStore.BlacklistEntry> entries = stateStore.listBlacklistEntries(clusterId);
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(toBlacklistEntryJson(entries.get(i)));
        }
        builder.append("]");
        return "{\"records\":" + builder + "}";
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

    private static EmbeddedAdminStateStore.BridgeConfig parseBridgeConfig(String body, EmbeddedAdminStateStore.BridgeConfig current) {
        List<String> types = extractStringList(body, "types");
        List<String> topicFilters = extractStringList(body, "topicFilters");
        boolean asyncEnabled = extractBoolean(body, "asyncEnabled", current.asyncEnabled());
        int asyncQueueCapacity = extractInt(body, "asyncQueueCapacity", current.asyncQueueCapacity());
        int asyncWorkerCount = extractInt(body, "asyncWorkerCount", current.asyncWorkerCount());
        if (types.isEmpty() && !containsField(body, "types")) {
            types = current.types();
        }
        if (topicFilters.isEmpty() && !containsField(body, "topicFilters")) {
            topicFilters = current.topicFilters();
        }
        EmbeddedAdminStateStore.BridgeKafkaConfig kafka = parseBridgeKafkaConfig(body, current.kafka());
        EmbeddedAdminStateStore.BridgeRocketmqConfig rocketmq = parseBridgeRocketmqConfig(body, current.rocketmq());
        EmbeddedAdminStateStore.BridgeMysqlConfig mysql = parseBridgeMysqlConfig(body, current.mysql());
        boolean enabled = extractBoolean(
                body,
                "enabled",
                kafka.enabled() || rocketmq.enabled() || mysql.enabled()
        );
        return new EmbeddedAdminStateStore.BridgeConfig(
                enabled,
                types,
                topicFilters,
                asyncEnabled,
                asyncQueueCapacity,
                asyncWorkerCount,
                kafka,
                rocketmq,
                mysql
        );
    }

    private static EmbeddedAdminStateStore.SecurityConfig parseSecurityConfig(String body, EmbeddedAdminStateStore.SecurityConfig current) {
        boolean aclEnabled = extractBoolean(body, "aclEnabled", current.aclEnabled());
        boolean authEnabled = extractBoolean(body, "authEnabled", current.authEnabled());
        List<String> aclChain = extractStringList(body, "aclChain");
        List<String> authChain = extractStringList(body, "authChain");
        long cacheTtlMs = extractLong(body, "cacheTtlMs", current.cacheTtlMs());
        if (aclChain.isEmpty() && !containsField(body, "aclChain")) {
            aclChain = current.aclChain();
        }
        if (authChain.isEmpty() && !containsField(body, "authChain")) {
            authChain = current.authChain();
        }
        boolean aclDefaultAllow = extractBoolean(body, "aclDefaultAllow", current.aclDefaultAllow());
        EmbeddedAdminStateStore.AclHttpConfig aclHttp = parseAclHttpConfig(body, current.aclHttp());
        EmbeddedAdminStateStore.AclFileConfig aclFile = parseAclFileConfig(body, current.aclFile());
        EmbeddedAdminStateStore.AclRedisConfig aclRedis = parseAclRedisConfig(body, current.aclRedis());
        EmbeddedAdminStateStore.AuthHttpConfig authHttp = parseAuthHttpConfig(body, current.authHttp());
        EmbeddedAdminStateStore.AuthFileConfig authFile = parseAuthFileConfig(body, current.authFile());
        EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig authBuiltInDatabase = parseAuthBuiltInDatabaseConfig(body, current.authBuiltInDatabase());
        EmbeddedAdminStateStore.AuthRedisConfig authRedis = parseAuthRedisConfig(body, current.authRedis());
        EmbeddedAdminStateStore.AuthMysqlConfig authMysql = parseAuthMysqlConfig(body, current.authMysql());
        EmbeddedAdminStateStore.AuthPostgresqlConfig authPostgresql = parseAuthPostgresqlConfig(body, current.authPostgresql());
        return new EmbeddedAdminStateStore.SecurityConfig(
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

    private EmbeddedAdminStateStore.ClientSnapshot parseClientSnapshot(String body) {
        return new EmbeddedAdminStateStore.ClientSnapshot(
                normalize(extractString(body, "clientId"), ""),
                normalize(extractString(body, "nodeId"), "unknown"),
                normalize(extractString(body, "clientIp"), "unknown"),
                Math.max(0, extractInt(body, "keepAliveSeconds", 0)),
                normalize(extractString(body, "connectionType"), ""),
                normalize(extractString(body, "username"), ""),
                Math.max(0L, extractLong(body, "connectedAt", System.currentTimeMillis())),
                List.of()
        );
    }

    private EmbeddedAdminStateStore.BlacklistEntry parseBlacklistEntry(String body, String source) {
        String type = normalize(extractString(body, "type"), "clientId");
        String value = normalize(extractString(body, "value"), "");
        if (value.isBlank()) {
            return null;
        }
        return new EmbeddedAdminStateStore.BlacklistEntry(type, value, System.currentTimeMillis(), source);
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
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"", Pattern.DOTALL).matcher(body);
        if (matcher.find()) {
            return unescapeJsonString(matcher.group(1));
        }
        return null;
    }

    private static String unescapeJsonString(String value) {
        if (value == null) {
            return null;
        }
        return value
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t")
                .replace("\\\"", "\"")
                .replace("\\\\", "\\");
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

    private static boolean containsField(String body, String key) {
        return Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:").matcher(body).find();
    }

    private static EmbeddedAdminStateStore.AclHttpConfig parseAclHttpConfig(
            String body,
            EmbeddedAdminStateStore.AclHttpConfig current
    ) {
        String segment = extractObject(body, "aclHttp");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AclHttpConfig(
                normalize(extractString(segment, "url"), current.url()),
                extractInt(segment, "timeoutMs", current.timeoutMs()),
                extractString(segment, "bodyTemplate") == null ? current.bodyTemplate() : extractString(segment, "bodyTemplate")
        );
    }

    private static EmbeddedAdminStateStore.BridgeKafkaConfig parseBridgeKafkaConfig(
            String body,
            EmbeddedAdminStateStore.BridgeKafkaConfig current
    ) {
        String segment = extractObject(body, "kafka");
        if (segment == null) {
            return current;
        }
        List<String> filters = extractStringList(segment, "sourceTopicFilters");
        if (filters.isEmpty() && !containsField(segment, "sourceTopicFilters")) {
            filters = current.sourceTopicFilters();
        }
        return new EmbeddedAdminStateStore.BridgeKafkaConfig(
                extractBoolean(segment, "enabled", current.enabled()),
                normalize(extractString(segment, "bootstrapServers"), current.bootstrapServers()),
                normalize(extractString(segment, "topic"), current.topic()),
                filters,
                normalize(extractString(segment, "acks"), current.acks()),
                normalize(extractString(segment, "clientId"), current.clientId()),
                normalize(extractString(segment, "compressionType"), current.compressionType())
        );
    }

    private static EmbeddedAdminStateStore.BridgeRocketmqConfig parseBridgeRocketmqConfig(
            String body,
            EmbeddedAdminStateStore.BridgeRocketmqConfig current
    ) {
        String segment = extractObject(body, "rocketmq");
        if (segment == null) {
            return current;
        }
        List<String> filters = extractStringList(segment, "sourceTopicFilters");
        if (filters.isEmpty() && !containsField(segment, "sourceTopicFilters")) {
            filters = current.sourceTopicFilters();
        }
        return new EmbeddedAdminStateStore.BridgeRocketmqConfig(
                extractBoolean(segment, "enabled", current.enabled()),
                normalize(extractString(segment, "nameServer"), current.nameServer()),
                normalize(extractString(segment, "producerGroup"), current.producerGroup()),
                normalize(extractString(segment, "topic"), current.topic()),
                filters,
                extractBoolean(segment, "syncSend", current.syncSend()),
                extractInt(segment, "timeoutMs", current.timeoutMs())
        );
    }

    private static EmbeddedAdminStateStore.BridgeMysqlConfig parseBridgeMysqlConfig(
            String body,
            EmbeddedAdminStateStore.BridgeMysqlConfig current
    ) {
        String segment = extractObject(body, "mysql");
        if (segment == null) {
            return current;
        }
        List<String> filters = extractStringList(segment, "sourceTopicFilters");
        if (filters.isEmpty() && !containsField(segment, "sourceTopicFilters")) {
            filters = current.sourceTopicFilters();
        }
        String driver = extractString(segment, "driver");
        String password = extractString(segment, "password");
        return new EmbeddedAdminStateStore.BridgeMysqlConfig(
                extractBoolean(segment, "enabled", current.enabled()),
                driver == null ? current.driver() : driver,
                normalize(extractString(segment, "url"), current.url()),
                normalize(extractString(segment, "user"), current.user()),
                password == null ? current.password() : password,
                normalize(extractString(segment, "table"), current.table()),
                filters,
                extractBoolean(segment, "autoCreateTable", current.autoCreateTable())
        );
    }

    private static EmbeddedAdminStateStore.AclFileConfig parseAclFileConfig(
            String body,
            EmbeddedAdminStateStore.AclFileConfig current
    ) {
        String segment = extractObject(body, "aclFile");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AclFileConfig(
                normalize(extractString(segment, "path"), current.path())
        );
    }

    private static EmbeddedAdminStateStore.AclRedisConfig parseAclRedisConfig(
            String body,
            EmbeddedAdminStateStore.AclRedisConfig current
    ) {
        String segment = extractObject(body, "aclRedis");
        if (segment == null) {
            return current;
        }
        return new EmbeddedAdminStateStore.AclRedisConfig(
                normalize(extractString(segment, "host"), current.host()),
                extractInt(segment, "port", current.port()),
                extractString(segment, "password") == null ? current.password() : extractString(segment, "password"),
                extractInt(segment, "db", current.db()),
                normalize(extractString(segment, "keyPrefix"), current.keyPrefix()),
                extractInt(segment, "timeoutMs", current.timeoutMs())
        );
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
                normalize(extractString(segment, "method"), current.method()),
                normalize(extractString(segment, "url"), current.url()),
                normalize(extractString(segment, "headersText"), current.headersText()),
                extractBoolean(segment, "tlsEnabled", current.tlsEnabled()),
                normalize(extractString(segment, "bodyTemplate"), current.bodyTemplate()),
                extractInt(segment, "poolSize", current.poolSize()),
                extractInt(segment, "rateLimitPerSecond", current.rateLimitPerSecond()),
                extractInt(segment, "requestTimeoutMs", extractInt(segment, "timeoutMs", current.requestTimeoutMs())),
                extractInt(segment, "connectTimeoutMs", current.connectTimeoutMs()),
                extractInt(segment, "pipelineCount", current.pipelineCount())
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

    private static String validateBridgeConfig(EmbeddedAdminStateStore.BridgeConfig config) {
        if (config == null || !config.enabled()) {
            return null;
        }
        if (config.types().isEmpty()) {
            return "已启用桥接时，至少需要选择一种桥接类型";
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

    private boolean isAuthRequired() {
        return adminAuthRuntime.isAuthRequired();
    }

    private AdminPrincipal resolveAuthorizedPrincipal(HttpExchange exchange) {
        if (!isAuthRequired()) {
            return new AdminPrincipal("anonymous", "super_admin");
        }
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        if (authorization == null || !authorization.regionMatches(true, 0, "Basic ", 0, 6)) {
            return null;
        }
        String encoded = authorization.substring(6).trim();
        if (encoded.isBlank()) {
            return null;
        }
        try {
            String decoded = new String(java.util.Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
            int split = decoded.indexOf(':');
            if (split < 0) {
                return null;
            }
            String username = decoded.substring(0, split);
            String password = decoded.substring(split + 1);
            if (adminAuthRuntime.matches(username, password)) {
                return new AdminPrincipal(username, adminAuthRuntime.current().role());
            }
            return null;
        } catch (IllegalArgumentException ignored) {
            return null;
        }
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

    private record AdminPrincipal(
            String username,
            String role
    ) {
        private boolean superAdmin() {
            return "super_admin".equalsIgnoreCase(role);
        }
    }

    private static String resolveAdminSource(HttpExchange exchange, AdminPrincipal principal) {
        String username = principal == null ? "anonymous" : normalize(principal.username(), "anonymous");
        return username + "@" + resolveAuditSource(exchange);
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
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t")
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

    @FunctionalInterface
    public interface BridgeConfigUpdater {
        void apply(String clusterId, EmbeddedAdminStateStore.BridgeConfig config);
    }

    @FunctionalInterface
    public interface ClientKickUpdater {
        void apply(String clusterId, String clientId);
    }

    public interface BlacklistUpdater {
        default void upsert(String clusterId, EmbeddedAdminStateStore.BlacklistEntry entry) {
        }

        default void delete(String clusterId, String type, String value) {
        }
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
