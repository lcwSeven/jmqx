package com.jmqx.admin;

import com.jmqx.admin.embedded.AdminAuthRuntime;
import com.jmqx.broker.core.SecurityPipelineMetrics;
import com.jmqx.session.SessionRegistry;
import com.jmqx.transport.ConnectionMetrics;
import okhttp3.Dispatcher;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    private final String baseUrl;
    private final String clusterId;
    private final AdminAuthRuntime adminAuthRuntime;
    private final String nodeId;
    private final String nodeIp;
    private final ConnectionMetrics connectionMetrics;
    private final SessionRegistry sessionRegistry;
    private final long metricsIntervalMs;
    private final OkHttpClient httpClient;
    private final ExecutorService requestExecutor;
    private final ScheduledExecutorService scheduler;
    private volatile Supplier<SecurityPipelineMetrics.Snapshot> securityMetricsSupplier;

    public HttpAdminReporter(String baseUrl,
                             String clusterId,
                             AdminAuthRuntime adminAuthRuntime,
                             String nodeId,
                             String nodeIp,
                             SessionRegistry sessionRegistry,
                             ConnectionMetrics connectionMetrics,
                             long connectTimeoutMs,
                             long requestTimeoutMs,
                             long metricsIntervalMs) {
        this.baseUrl = stripTrailingSlash(Objects.requireNonNull(baseUrl, "baseUrl"));
        this.clusterId = normalize(clusterId, "default");
        this.adminAuthRuntime = Objects.requireNonNull(adminAuthRuntime, "adminAuthRuntime");
        this.nodeId = normalize(nodeId, "node-1");
        this.nodeIp = normalize(nodeIp, "unknown");
        this.sessionRegistry = sessionRegistry;
        this.connectionMetrics = Objects.requireNonNull(connectionMetrics, "connectionMetrics");
        this.metricsIntervalMs = Math.max(metricsIntervalMs, 1000);
        long effectiveRequestTimeoutMs = Math.max(requestTimeoutMs, 500);
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
        this.httpClient = new OkHttpClient.Builder()
                .dispatcher(new Dispatcher(requestExecutor))
                .connectTimeout(Duration.ofMillis(Math.max(connectTimeoutMs, 500)))
                .callTimeout(Duration.ofMillis(effectiveRequestTimeoutMs))
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
        upsertNodeMetrics(nodeId, nodeIp, inboundBytes, outboundBytes, connectedClients, reportTime, null);
    }

    private void upsertNodeMetrics(String nodeId,
                                  String nodeIp,
                                  long inboundBytes,
                                  long outboundBytes,
                                  int connectedClients,
                                  long reportTime,
                                  SecurityPipelineMetrics.Snapshot securityMetrics) {
        String body = "{"
                + "\"nodeIp\":\"" + escape(nodeIp) + "\","
                + "\"inboundBytes\":" + inboundBytes + ","
                + "\"outboundBytes\":" + outboundBytes + ","
                + "\"connectedClients\":" + connectedClients + ","
                + "\"connectAuthSuccess\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::connectAuthSuccess) + ","
                + "\"connectAuthFailure\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::connectAuthFailure) + ","
                + "\"connectAuthError\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::connectAuthError) + ","
                + "\"connectAuthSlow\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::connectAuthSlow) + ","
                + "\"connectAuthAvgMs\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::connectAuthAvgMs) + ","
                + "\"connectAuthMaxMs\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::connectAuthMaxMs) + ","
                + "\"publishAclAllow\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::publishAclAllow) + ","
                + "\"publishAclDeny\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::publishAclDeny) + ","
                + "\"publishAclError\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::publishAclError) + ","
                + "\"publishAclSlow\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::publishAclSlow) + ","
                + "\"publishAclAvgMs\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::publishAclAvgMs) + ","
                + "\"publishAclMaxMs\":" + securityMetricValue(securityMetrics, SecurityPipelineMetrics.Snapshot::publishAclMaxMs) + ","
                + "\"reportTime\":" + reportTime
                + "}";
        requestAsync("POST", "/api/v1/internal/nodes/" + url(nodeId) + "/metrics", body);
    }

    @Override
    public void setSecurityMetricsSupplier(Supplier<SecurityPipelineMetrics.Snapshot> supplier) {
        this.securityMetricsSupplier = supplier;
    }

    @Override
    public void shutdown() {
        scheduler.shutdownNow();
        httpClient.dispatcher().cancelAll();
        httpClient.connectionPool().evictAll();
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
            Supplier<SecurityPipelineMetrics.Snapshot> supplier = securityMetricsSupplier;
            SecurityPipelineMetrics.Snapshot snapshot = supplier == null ? null : supplier.get();
            upsertNodeMetrics(
                    nodeId,
                    nodeIp,
                    connectionMetrics.getInboundBytes(),
                    connectionMetrics.getOutboundBytes(),
                    sessionRegistry == null ? 0 : sessionRegistry.list().size(),
                    System.currentTimeMillis(),
                    snapshot
            );
        } catch (Exception exception) {
            LOG.log(Level.FINE, "report node metrics failed: " + exception.getMessage(), exception);
        }
    }

    private void requestAsync(String method, String path, String body) {
        String url = baseUrl + path + "?clusterId=" + url(clusterId);
        try {
            Request.Builder builder = new Request.Builder()
                    .url(URI.create(url).toString());
            AdminAuthRuntime.Config adminAuthConfig = adminAuthRuntime.current();
            if (!adminAuthConfig.username().isBlank()) {
                builder.header("Authorization", okhttp3.Credentials.basic(adminAuthConfig.username(), adminAuthConfig.password()));
            }
            if ("DELETE".equals(method)) {
                builder.delete();
            } else if (body == null) {
                builder.method(method, null);
            } else {
                builder.header("Content-Type", "application/json");
                builder.method(method, RequestBody.create(body, JSON_MEDIA_TYPE));
            }
            httpClient.newCall(builder.build()).enqueue(new okhttp3.Callback() {
                @Override
                public void onFailure(okhttp3.Call call, java.io.IOException exception) {
                    LOG.fine(() -> "[ADMIN] request error url=" + url + ", error=" + exception.getMessage());
                }

                @Override
                public void onResponse(okhttp3.Call call, Response response) throws IOException {
                    try (response) {
                        if (response.code() >= 300) {
                            String responseBody = response.body() == null ? "" : response.body().string();
                            LOG.warning(() -> "[ADMIN] request failed status=" + response.code() + ", url=" + url + ", body=" + responseBody);
                        }
                    }
                }
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

    private static long securityMetricValue(
            SecurityPipelineMetrics.Snapshot snapshot,
            java.util.function.ToLongFunction<SecurityPipelineMetrics.Snapshot> extractor
    ) {
        if (snapshot == null || extractor == null) {
            return 0L;
        }
        return Math.max(0L, extractor.applyAsLong(snapshot));
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
