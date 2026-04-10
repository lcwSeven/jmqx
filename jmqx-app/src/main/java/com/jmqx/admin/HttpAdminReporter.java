package com.jmqx.admin;

import com.jmqx.transport.ConnectionMetrics;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 HTTP 的管理端上报器。
 *
 * @author liucaiwen
 * @since 2026-04-10
 */
public class HttpAdminReporter implements AdminReporter {

    private static final Logger LOG = Logger.getLogger(HttpAdminReporter.class.getName());

    private final String baseUrl;
    private final String clusterId;
    private final String nodeId;
    private final String nodeIp;
    private final ConnectionMetrics connectionMetrics;
    private final long metricsIntervalMs;
    private final long requestTimeoutMs;
    private final HttpClient httpClient;
    private final ExecutorService requestExecutor;
    private final ScheduledExecutorService scheduler;

    public HttpAdminReporter(String baseUrl,
                             String clusterId,
                             String nodeId,
                             String nodeIp,
                             ConnectionMetrics connectionMetrics,
                             long connectTimeoutMs,
                             long requestTimeoutMs,
                             long metricsIntervalMs) {
        this.baseUrl = stripTrailingSlash(Objects.requireNonNull(baseUrl, "baseUrl"));
        this.clusterId = normalize(clusterId, "default");
        this.nodeId = normalize(nodeId, "node-1");
        this.nodeIp = normalize(nodeIp, "unknown");
        this.connectionMetrics = Objects.requireNonNull(connectionMetrics, "connectionMetrics");
        this.metricsIntervalMs = Math.max(metricsIntervalMs, 1000);
        this.requestTimeoutMs = Math.max(requestTimeoutMs, 500);
        this.requestExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "jmqx-admin-reporter");
            thread.setDaemon(true);
            return thread;
        });
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "jmqx-admin-metrics");
            thread.setDaemon(true);
            return thread;
        });
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Math.max(connectTimeoutMs, 500)))
                .executor(requestExecutor)
                .build();
        scheduler.scheduleWithFixedDelay(this::reportNodeMetricsSafely, 1000, this.metricsIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void upsertClientSession(String clientId,
                                    String nodeId,
                                    String clientIp,
                                    int keepAliveSeconds,
                                    String connectionType,
                                    String username,
                                    long connectedAtEpochMs) {
        String body = "{"
                + "\"clientId\":\"" + escape(clientId) + "\","
                + "\"nodeId\":\"" + escape(nodeId) + "\","
                + "\"clientIp\":\"" + escape(clientIp) + "\","
                + "\"keepAliveSeconds\":" + keepAliveSeconds + ","
                + "\"connectionType\":\"" + escape(connectionType) + "\","
                + "\"username\":\"" + escape(username) + "\","
                + "\"connectedAt\":" + connectedAtEpochMs
                + "}";
        requestAsync("POST", "/api/v1/internal/clients", body);
    }

    @Override
    public void removeClientSession(String clientId) {
        requestAsync("DELETE", "/api/v1/internal/clients/" + url(clientId), null);
    }

    @Override
    public void upsertClientSubscriptions(String clientId, List<String> topics) {
        if (topics == null) {
            topics = List.of();
        }
        StringBuilder builder = new StringBuilder();
        builder.append("{\"topics\":[");
        for (int i = 0; i < topics.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append("\"").append(escape(topics.get(i))).append("\"");
        }
        builder.append("]}");
        requestAsync("POST", "/api/v1/internal/clients/" + url(clientId) + "/subscriptions", builder.toString());
    }

    @Override
    public void upsertNodeMetrics(String nodeId,
                                  String nodeIp,
                                  long inboundBytes,
                                  long outboundBytes,
                                  int connectedClients,
                                  long reportTime) {
        String body = "{"
                + "\"nodeIp\":\"" + escape(nodeIp) + "\","
                + "\"inboundBytes\":" + inboundBytes + ","
                + "\"outboundBytes\":" + outboundBytes + ","
                + "\"connectedClients\":" + connectedClients + ","
                + "\"reportTime\":" + reportTime
                + "}";
        requestAsync("POST", "/api/v1/internal/nodes/" + url(nodeId) + "/metrics", body);
    }

    @Override
    public void shutdown() {
        scheduler.shutdownNow();
        requestExecutor.shutdown();
        try {
            if (!requestExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                requestExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            requestExecutor.shutdownNow();
        }
    }

    private void reportNodeMetricsSafely() {
        try {
            upsertNodeMetrics(
                    nodeId,
                    nodeIp,
                    connectionMetrics.getInboundBytes(),
                    connectionMetrics.getOutboundBytes(),
                    connectionMetrics.getActiveConnections(),
                    System.currentTimeMillis()
            );
        } catch (Exception exception) {
            LOG.log(Level.FINE, "report node metrics failed: " + exception.getMessage(), exception);
        }
    }

    private void requestAsync(String method, String path, String body) {
        String url = baseUrl + path + "?clusterId=" + url(clusterId);
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMillis(requestTimeoutMs));
            if ("DELETE".equals(method)) {
                builder.DELETE();
            } else if (body == null) {
                builder.method(method, HttpRequest.BodyPublishers.noBody());
            } else {
                builder.header("Content-Type", "application/json");
                builder.method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
            }
            httpClient.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() >= 300) {
                            LOG.warning(() -> "[ADMIN] request failed status=" + response.statusCode() + ", url=" + url + ", body=" + response.body());
                        }
                    })
                    .exceptionally(exception -> {
                        LOG.fine(() -> "[ADMIN] request error url=" + url + ", error=" + exception.getMessage());
                        return null;
                    });
        } catch (Exception exception) {
            LOG.fine(() -> "[ADMIN] request build failed url=" + url + ", error=" + exception.getMessage());
        }
    }

    private static String normalize(String value, String defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static String stripTrailingSlash(String value) {
        if (value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String url(String value) {
        return URLEncoder.encode(normalize(value, ""), StandardCharsets.UTF_8);
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}
