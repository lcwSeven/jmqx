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
        try {
            String body = "{"
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
            return responseBody.contains("\"allow\":true") || "allow".equals(responseBody) || "true".equals(responseBody);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.log(Level.WARNING, "HTTP auth request failed: " + e.getMessage(), e);
            return false;
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "HTTP auth runtime error: " + e.getMessage(), e);
            return false;
        }
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
