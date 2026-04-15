package com.jmqx.acl;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class HttpAclAuthorizer implements AclAuthorizer {
    private static final Logger LOG = Logger.getLogger(HttpAclAuthorizer.class.getName());
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    private final OkHttpClient httpClient;
    private final URI endpoint;
    private final String bodyTemplate;

    public HttpAclAuthorizer(AclProperties properties) {
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofMillis(Math.max(properties.getHttpTimeoutMs(), 200)))
                .callTimeout(Duration.ofMillis(Math.max(properties.getHttpTimeoutMs(), 200)))
                .build();
        this.endpoint = URI.create(properties.getHttpUrl());
        this.bodyTemplate = normalizeBodyTemplate(properties.getHttpBodyTemplate());
    }

    @Override
    public AclDecision authorize(AclRequest request) {
        try {
            return authorizeAsync(request).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return AclDecision.NOT_FOUND;
        } catch (Exception exception) {
            LOG.log(Level.WARNING, "HTTP ACL request failed: " + exception.getMessage(), exception);
            return AclDecision.NOT_FOUND;
        }
    }

    @Override
    public CompletableFuture<AclDecision> authorizeAsync(AclRequest request) {
        CompletableFuture<AclDecision> future = new CompletableFuture<>();
        try {
            String body = renderTemplate(bodyTemplate, request);
            Request httpRequest = new Request.Builder()
                .url(endpoint.toString())
                .header("Content-Type", "application/json")
                .post(RequestBody.create(body, JSON_MEDIA_TYPE))
                .build();
            httpClient.newCall(httpRequest).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    LOG.log(Level.WARNING, "HTTP ACL request failed: " + e.getMessage(), e);
                    future.complete(AclDecision.NOT_FOUND);
                }

                @Override
                public void onResponse(Call call, Response response) {
                    try (response) {
                        future.complete(parseAclDecision(response));
                    } catch (Exception e) {
                        LOG.log(Level.WARNING, "HTTP ACL response parse failed: " + e.getMessage(), e);
                        future.complete(AclDecision.NOT_FOUND);
                    }
                }
            });
            return future;
        } catch (RuntimeException exception) {
            LOG.log(Level.WARNING, "HTTP ACL runtime error: " + exception.getMessage(), exception);
            future.complete(AclDecision.NOT_FOUND);
            return future;
        }
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String normalizeBodyTemplate(String template) {
        if (template == null || template.isBlank()) {
            return """
                    {
                      "clientId": "${clientId}",
                      "username": "${username}",
                      "topic": "${topic}",
                      "action": "${action}"
                    }
                    """;
        }
        return template;
    }

    private static String renderTemplate(String template, AclRequest request) {
        return normalizePlaceholderEscapes(normalizeBodyTemplate(template))
                .replace("${clientId}", escape(request.getClientId()))
                .replace("${username}", escape(request.getUsername()))
                .replace("${topic}", escape(request.getTopic()))
                .replace("${action}", request.getAction() == null ? "" : request.getAction().name().toLowerCase(Locale.ROOT));
    }

    private static String normalizePlaceholderEscapes(String template) {
        return template
                .replace("\\${clientId}", "${clientId}")
                .replace("\\${username}", "${username}")
                .replace("\\${topic}", "${topic}")
                .replace("\\${action}", "${action}");
    }

    private static AclDecision parseAclDecision(Response response) throws IOException {
        String responseBody = response.body() == null ? "" : response.body().string().trim().toLowerCase();
        if (responseBody.contains("\"allow\":true") || "allow".equals(responseBody) || "true".equals(responseBody)) {
            return AclDecision.ALLOW;
        }
        if (responseBody.contains("\"allow\":false") || "deny".equals(responseBody) || "false".equals(responseBody)) {
            return AclDecision.DENY;
        }
        return AclDecision.NOT_FOUND;
    }
}
