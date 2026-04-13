package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class HttpAuthProvider implements AuthProvider {
    private static final Logger LOG = Logger.getLogger(HttpAuthProvider.class.getName());
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    private final ExecutorService executorService;
    private final OkHttpClient httpClient;
    private final String method;
    private final URI endpoint;
    private final Map<String, String> headers;
    private final String bodyTemplate;
    private final Semaphore pipelineSemaphore;
    private final RequestRateLimiter rateLimiter;

    public HttpAuthProvider(AuthProperties properties) {
        int poolSize = Math.max(properties.getHttpPoolSize(), 1);
        this.executorService = Executors.newFixedThreadPool(poolSize, runnable -> {
            Thread thread = new Thread(runnable, "jmqx-auth-http");
            thread.setDaemon(true);
            return thread;
        });
        this.httpClient = new OkHttpClient.Builder()
                .dispatcher(new okhttp3.Dispatcher(executorService))
                .connectTimeout(Duration.ofMillis(Math.max(properties.getHttpConnectTimeoutMs(), 200)))
                .callTimeout(Duration.ofMillis(Math.max(properties.getHttpRequestTimeoutMs(), 200)))
                .build();
        this.method = normalizeMethod(properties.getHttpMethod());
        this.endpoint = URI.create(resolveEndpoint(properties.getHttpUrl(), properties.isHttpTlsEnabled()));
        this.headers = parseHeaders(properties.getHttpHeaders());
        this.bodyTemplate = normalizeBodyTemplate(properties.getHttpBodyTemplate());
        this.pipelineSemaphore = new Semaphore(Math.max(1, poolSize * Math.max(properties.getHttpPipelineCount(), 1)));
        this.rateLimiter = new RequestRateLimiter(properties.getHttpRateLimitPerSecond());
    }

    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        boolean acquired = false;
        try {
            pipelineSemaphore.acquire();
            acquired = true;
            if (!rateLimiter.tryAcquire()) {
                LOG.fine(() -> "[AUTH-HTTP] request rejected by rate limiter, endpoint=" + endpoint);
                return AuthResult.deny();
            }
            String body = renderTemplate(bodyTemplate, request);
            Request requestObject = buildRequest(body);
            try (Response response = httpClient.newCall(requestObject).execute()) {
                String responseBody = response.body() == null ? "" : response.body().string().trim().toLowerCase();
                if (responseBody.contains("\"allow\":true") || "allow".equals(responseBody) || "true".equals(responseBody)) {
                    return AuthResult.allow();
                }
                if (responseBody.contains("\"notfound\":true")
                    || responseBody.contains("\"not_found\":true")
                    || "not_found".equals(responseBody)
                    || "notfound".equals(responseBody)) {
                    return AuthResult.notFound();
                }
                return AuthResult.deny();
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.log(Level.WARNING, "HTTP auth request failed: " + e.getMessage(), e);
            return AuthResult.deny();
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "HTTP auth runtime error: " + e.getMessage(), e);
            return AuthResult.deny();
        } finally {
            if (acquired) {
                pipelineSemaphore.release();
            }
        }
    }

    @Override
    public void close() {
        httpClient.dispatcher().cancelAll();
        httpClient.connectionPool().evictAll();
        executorService.shutdownNow();
    }

    private Request buildRequest(String body) {
        Request.Builder builder = new Request.Builder()
                .url(endpoint.toString())
                .tag(HttpAuthProvider.class)
                .addHeader("Content-Type", "application/json");
        headers.forEach(builder::header);
        if ("GET".equals(method)) {
            return builder.get().build();
        }
        RequestBody requestBody = RequestBody.create(body, JSON_MEDIA_TYPE);
        if ("PUT".equals(method)) {
            return builder.put(requestBody).build();
        }
        LOG.info("[AUTH-HTTP] request method={}" + method);
        return builder.post(requestBody).build();
    }

    private static String normalizeMethod(String value) {
        String method = value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
        if ("GET".equals(method) || "PUT".equals(method)) {
            return method;
        }
        return "POST";
    }

    private static String resolveEndpoint(String rawUrl, boolean tlsEnabled) {
        String url = rawUrl == null ? "" : rawUrl.trim();
        if (tlsEnabled && url.startsWith("http://")) {
            return "https://" + url.substring("http://".length());
        }
        return url;
    }

    private static Map<String, String> parseHeaders(String rawHeaders) {
        Map<String, String> result = new LinkedHashMap<>();
        String content = rawHeaders == null ? "" : rawHeaders;
        String[] lines = content.split("\\r?\\n");
        for (String line : lines) {
            if (line == null || line.isBlank()) {
                continue;
            }
            int split = line.indexOf(':');
            if (split <= 0) {
                continue;
            }
            String key = line.substring(0, split).trim();
            String value = line.substring(split + 1).trim();
            if (!key.isBlank()) {
                result.put(key, value);
            }
        }
        if (!result.containsKey("Content-Type") && !result.containsKey("content-type")) {
            result.put("Content-Type", "application/json");
        }
        return result;
    }

    private static String normalizeBodyTemplate(String template) {
        if (template == null || template.isBlank()) {
            return "{\n  \"username\": \"${username}\",\n  \"password\": \"${password}\"\n}";
        }
        return template;
    }

    private static String renderTemplate(String template, AuthRequest request) {
        return normalizePlaceholderEscapes(normalizeBodyTemplate(template))
                .replace("${clientId}", escape(request.getClientId()))
                .replace("${username}", escape(request.getUsername()))
                .replace("${password}", escape(request.getPassword()));
    }

    private static String normalizePlaceholderEscapes(String template) {
        return template
                .replace("\\${clientId}", "${clientId}")
                .replace("\\${username}", "${username}")
                .replace("\\${password}", "${password}");
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static final class RequestRateLimiter {
        private final int permitsPerSecond;
        private double storedPermits;
        private long lastRefillNanos;

        private RequestRateLimiter(int permitsPerSecond) {
            this.permitsPerSecond = Math.max(permitsPerSecond, 0);
            this.storedPermits = this.permitsPerSecond;
            this.lastRefillNanos = System.nanoTime();
        }

        private synchronized boolean tryAcquire() {
            if (permitsPerSecond <= 0) {
                return true;
            }
            refill();
            if (storedPermits < 1.0d) {
                return false;
            }
            storedPermits -= 1.0d;
            return true;
        }

        private void refill() {
            long now = System.nanoTime();
            if (now <= lastRefillNanos) {
                return;
            }
            double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000d;
            storedPermits = Math.min(permitsPerSecond, storedPermits + elapsedSeconds * permitsPerSecond);
            lastRefillNanos = now;
        }
    }
}
