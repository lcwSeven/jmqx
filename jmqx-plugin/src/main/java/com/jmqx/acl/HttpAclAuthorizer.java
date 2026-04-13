package com.jmqx.acl;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
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
    private final boolean defaultAllow;

    public HttpAclAuthorizer(AclProperties properties) {
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofMillis(Math.max(properties.getHttpTimeoutMs(), 200)))
                .callTimeout(Duration.ofMillis(Math.max(properties.getHttpTimeoutMs(), 200)))
                .build();
        this.endpoint = URI.create(properties.getHttpUrl());
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
            Request httpRequest = new Request.Builder()
                .url(endpoint.toString())
                .header("Content-Type", "application/json")
                .post(RequestBody.create(body, JSON_MEDIA_TYPE))
                .build();
            try (Response response = httpClient.newCall(httpRequest).execute()) {
                String responseBody = response.body() == null ? "" : response.body().string().trim().toLowerCase();
                if (responseBody.contains("\"allow\":true") || "allow".equals(responseBody) || "true".equals(responseBody)) {
                    return true;
                }
                if (responseBody.contains("\"allow\":false") || "deny".equals(responseBody) || "false".equals(responseBody)) {
                    return false;
                }
                return defaultAllow;
            }
        } catch (IOException e) {
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
