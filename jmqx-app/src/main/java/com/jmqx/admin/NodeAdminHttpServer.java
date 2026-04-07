package com.jmqx.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.session.ClientSession;
import com.jmqx.session.SessionRegistry;
import com.jmqx.transport.ConnectionMetrics;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 当前 broker 节点暴露的轻量管理接口。
 * 独立 admin 通过这里采集状态、客户端列表和下发热更新配置。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class NodeAdminHttpServer {
    private static final Logger LOG = Logger.getLogger(NodeAdminHttpServer.class.getName());

    private final NodeAdminProperties properties;
    private final ConnectionMetrics connectionMetrics;
    private final RuntimeConfigService runtimeConfigService;
    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private final String nodeId;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private HttpServer httpServer;

    public NodeAdminHttpServer(
        NodeAdminProperties properties,
        ConnectionMetrics connectionMetrics,
        RuntimeConfigService runtimeConfigService,
        SessionRegistry sessionRegistry,
        SubscriptionRegistry subscriptionRegistry,
        String nodeId
    ) {
        this.properties = properties;
        this.connectionMetrics = connectionMetrics;
        this.runtimeConfigService = runtimeConfigService;
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.nodeId = nodeId;
    }

    public synchronized void start() {
        if (httpServer != null) {
            return;
        }
        try {
            httpServer = HttpServer.create(new InetSocketAddress(properties.getHost(), properties.getPort()), 128);
            httpServer.createContext("/api/admin", this::handle);
            httpServer.setExecutor(null);
            httpServer.start();
            LOG.info(() -> "[NODE-ADMIN] started at " + properties.getHost() + ":" + properties.getPort());
        } catch (IOException e) {
            throw new IllegalStateException("failed to start node admin server", e);
        }
    }

    public synchronized void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            String route = path.substring("/api/admin".length());
            if (route.isEmpty()) {
                route = "/";
            }

            if ("GET".equalsIgnoreCase(method) && "/status".equals(route)) {
                writeJson(exchange, 200, buildStatus());
                return;
            }
            if ("GET".equalsIgnoreCase(method) && "/clients".equals(route)) {
                writeJson(exchange, 200, listClients(exchange.getRequestURI()));
                return;
            }
            if ("GET".equalsIgnoreCase(method) && route.startsWith("/clients/")) {
                String rawClientId = route.substring("/clients/".length());
                String clientId = urlDecode(rawClientId);
                ClientDetailResponse detail = findClientDetail(clientId);
                if (detail == null) {
                    writeJson(exchange, 404, Map.of("message", "client not found: " + clientId));
                    return;
                }
                writeJson(exchange, 200, detail);
                return;
            }
            if ("POST".equalsIgnoreCase(method) && "/config".equals(route)) {
                // 这里直接接收完整配置快照并热更新，避免 MQTT 节点因为安全配置调整而重启。
                ConfigUpdateRequest request = objectMapper.readValue(exchange.getRequestBody(), ConfigUpdateRequest.class);
                runtimeConfigService.update(
                    request.authType,
                    request.authChain,
                    request.authCacheMillis,
                    request.authHttpUrl,
                    request.authHttpTimeoutMs,
                    request.authFilePath,
                    request.authRedisHost,
                    request.authRedisPort,
                    request.authRedisPassword,
                    request.authRedisDb,
                    request.authRedisKeyPrefix,
                    request.authRedisTimeoutMs,
                    request.authDbDriver,
                    request.authDbUrl,
                    request.authDbUser,
                    request.authDbPassword,
                    request.authDbQuery,
                    request.aclType,
                    request.aclCacheMillis,
                    request.aclDefaultAllow,
                    request.aclHttpUrl,
                    request.aclHttpTimeoutMs,
                    request.aclRedisHost,
                    request.aclRedisPort,
                    request.aclRedisPassword,
                    request.aclRedisDb,
                    request.aclRedisKeyPrefix,
                    request.aclRedisTimeoutMs,
                    request.aclFilePath
                );
                writeJson(exchange, 200, buildStatus());
                return;
            }
            writeJson(exchange, 404, Map.of("message", "not found"));
        } catch (Exception e) {
            LOG.log(Level.WARNING, "node admin request failed: " + e.getMessage(), e);
            writeJson(exchange, 500, Map.of("message", "internal error: " + e.getMessage()));
        } finally {
            exchange.close();
        }
    }

    private StatusResponse buildStatus() {
        StatusResponse response = new StatusResponse();
        response.nodeId = nodeId;
        response.connections = connectionMetrics.getActiveConnections();
        response.authType = runtimeConfigService.getAuthType();
        response.authCacheMillis = runtimeConfigService.getAuthCacheMillis();
        response.aclType = runtimeConfigService.getAclType();
        response.aclCacheMillis = runtimeConfigService.getAclCacheMillis();
        response.serverTimeEpochMillis = Instant.now().toEpochMilli();
        return response;
    }

    private List<ClientResponse> listClients(URI uri) {
        Map<String, String> params = parseQuery(uri.getRawQuery());
        String clientIdQuery = normalize(params.get("clientId"));
        String usernameQuery = normalize(params.get("username"));

        // 管理台聚合时会频繁轮询这里，因此只返回必要字段，保持接口轻量。
        return sessionRegistry.list().stream()
            .filter(session -> containsIgnoreCase(session.clientId(), clientIdQuery))
            .filter(session -> containsIgnoreCase(session.username(), usernameQuery))
            .sorted(Comparator.comparing(ClientSession::connectedAt).reversed())
            .map(this::toClientResponse)
            .toList();
    }

    private ClientDetailResponse findClientDetail(String clientId) {
        return sessionRegistry.get(clientId).map(session -> {
            ClientDetailResponse detail = new ClientDetailResponse();
            detail.clientId = session.clientId();
            detail.onlineAtEpochMillis = session.connectedAt().toEpochMilli();
            detail.username = session.username();
            detail.connectionType = session.connectionType();
            detail.serviceNodeIp = session.serviceNodeIp();
            detail.keepAliveSeconds = session.keepAliveSeconds();
            detail.subscriptions = toSubscriptionList(subscriptionRegistry.findSubscriptions(clientId));
            return detail;
        }).orElse(null);
    }

    private ClientResponse toClientResponse(ClientSession session) {
        ClientResponse response = new ClientResponse();
        response.clientId = session.clientId();
        response.onlineAtEpochMillis = session.connectedAt().toEpochMilli();
        response.username = session.username();
        response.connectionType = session.connectionType();
        response.serviceNodeIp = session.serviceNodeIp();
        response.keepAliveSeconds = session.keepAliveSeconds();
        return response;
    }

    private List<ClientSubscriptionResponse> toSubscriptionList(Map<String, Integer> subscriptions) {
        List<ClientSubscriptionResponse> result = new ArrayList<>();
        subscriptions.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            ClientSubscriptionResponse item = new ClientSubscriptionResponse();
            item.topic = entry.getKey();
            item.qos = entry.getValue() == null ? 0 : entry.getValue();
            result.add(item);
        });
        return result;
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> queryMap = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return queryMap;
        }
        String[] pairs = rawQuery.split("&");
        for (String pair : pairs) {
            if (pair == null || pair.isBlank()) {
                continue;
            }
            int idx = pair.indexOf('=');
            if (idx < 0) {
                queryMap.put(urlDecode(pair), "");
                continue;
            }
            String key = urlDecode(pair.substring(0, idx));
            String value = urlDecode(pair.substring(idx + 1));
            queryMap.put(key, value);
        }
        return queryMap;
    }

    private static String urlDecode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static boolean containsIgnoreCase(String value, String query) {
        if (query == null) {
            return true;
        }
        if (value == null) {
            return false;
        }
        return value.toLowerCase().contains(query.toLowerCase());
    }

    private void writeJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] payload = objectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, payload.length);
        exchange.getResponseBody().write(payload);
    }

    private static final class StatusResponse {
        public String nodeId;
        public int connections;
        public String authType;
        public int authCacheMillis;
        public String aclType;
        public int aclCacheMillis;
        public long serverTimeEpochMillis;
    }

    private static final class ClientResponse {
        public String clientId;
        public long onlineAtEpochMillis;
        public String username;
        public String connectionType;
        public String serviceNodeIp;
        public int keepAliveSeconds;
    }

    private static final class ClientDetailResponse {
        public String clientId;
        public long onlineAtEpochMillis;
        public String username;
        public String connectionType;
        public String serviceNodeIp;
        public int keepAliveSeconds;
        public List<ClientSubscriptionResponse> subscriptions;
    }

    private static final class ClientSubscriptionResponse {
        public String topic;
        public int qos;
    }

    private static final class ConfigUpdateRequest {
        public String authType;
        public String authChain;
        public Integer authCacheMillis;
        public String authHttpUrl;
        public Integer authHttpTimeoutMs;
        public String authFilePath;
        public String authRedisHost;
        public Integer authRedisPort;
        public String authRedisPassword;
        public Integer authRedisDb;
        public String authRedisKeyPrefix;
        public Integer authRedisTimeoutMs;
        public String authDbDriver;
        public String authDbUrl;
        public String authDbUser;
        public String authDbPassword;
        public String authDbQuery;

        public String aclType;
        public Integer aclCacheMillis;
        public Boolean aclDefaultAllow;
        public String aclHttpUrl;
        public Integer aclHttpTimeoutMs;
        public String aclRedisHost;
        public Integer aclRedisPort;
        public String aclRedisPassword;
        public Integer aclRedisDb;
        public String aclRedisKeyPrefix;
        public Integer aclRedisTimeoutMs;
        public String aclFilePath;
    }
}
