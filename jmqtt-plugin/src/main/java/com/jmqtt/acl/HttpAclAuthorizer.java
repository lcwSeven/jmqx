package com.jmqtt.acl;

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
public class HttpAclAuthorizer implements AclAuthorizer {
    private static final Logger LOG = Logger.getLogger(HttpAclAuthorizer.class.getName());

    private final HttpClient httpClient;
    private final URI endpoint;
    private final Duration timeout;
    private final boolean defaultAllow;

    public HttpAclAuthorizer(AclProperties properties) {
        this.httpClient = HttpClient.newHttpClient();
        this.endpoint = URI.create(properties.getHttpUrl());
        this.timeout = Duration.ofMillis(Math.max(properties.getHttpTimeoutMs(), 200));
        this.defaultAllow = properties.isDefaultAllow();
    }

    @Override
    public boolean isAllowed(AclRequest request) {
        try {
            String body = "{"
                + "\"clientId\":\"" + escape(request.getClientId()) + "\","
                + "\"username\":\"" + escape(request.getUsername()) + "\","
                + "\"topic\":\"" + escape(request.getTopic()) + "\","
                + "\"action\":\"" + request.getAction().name().toLowerCase() + "\""
                + "}";
            HttpRequest httpRequest = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            String responseBody = response.body() == null ? "" : response.body().trim().toLowerCase();
            if (responseBody.contains("\"allow\":true") || "allow".equals(responseBody) || "true".equals(responseBody)) {
                return true;
            }
            if (responseBody.contains("\"allow\":false") || "deny".equals(responseBody) || "false".equals(responseBody)) {
                return false;
            }
            return defaultAllow;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.log(Level.WARNING, "HTTP ACL request failed: " + e.getMessage(), e);
            return defaultAllow;
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "HTTP ACL runtime error: " + e.getMessage(), e);
            return defaultAllow;
        }
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
