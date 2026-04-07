package com.jmqx.auth;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class HttpAuthProvider implements AuthProvider {
    private static final Logger LOG = Logger.getLogger(HttpAuthProvider.class.getName());

    private final HttpClient httpClient;
    private final URI endpoint;
    private final Duration timeout;

    public HttpAuthProvider(AuthProperties properties) {
        this.httpClient = HttpClient.newHttpClient();
        this.endpoint = URI.create(properties.getHttpUrl());
        this.timeout = Duration.ofMillis(Math.max(properties.getHttpTimeoutMs(), 200));
    }

    @Override
    public boolean authenticate(AuthRequest request) {
        return authenticateDecision(request) == AuthDecision.ALLOW;
    }

    @Override
    public AuthDecision authenticateDecision(AuthRequest request) {
        try {
            String body = "{"
                + "\"clientId\":\"" + escape(request.getClientId()) + "\","
                + "\"username\":\"" + escape(request.getUsername()) + "\","
                + "\"password\":\"" + escape(request.getPassword()) + "\""
                + "}";
            HttpRequest httpRequest = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            String responseBody = response.body() == null ? "" : response.body().trim().toLowerCase();
            if (responseBody.contains("\"allow\":true") || "allow".equals(responseBody) || "true".equals(responseBody)) {
                return AuthDecision.ALLOW;
            }
            if (responseBody.contains("\"notfound\":true")
                || responseBody.contains("\"not_found\":true")
                || "not_found".equals(responseBody)
                || "notfound".equals(responseBody)) {
                return AuthDecision.NOT_FOUND;
            }
            return AuthDecision.DENY;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.log(Level.WARNING, "HTTP auth request failed: " + e.getMessage(), e);
            return AuthDecision.DENY;
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "HTTP auth runtime error: " + e.getMessage(), e);
            return AuthDecision.DENY;
        }
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
