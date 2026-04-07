package com.jmqx.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * admin 到 broker 节点的 HTTP 代理客户端。
 * admin 自己不保存节点运行态，只负责把请求代理到目标节点并补充节点元数据。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
@Service
public class NodeProxyClient {
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Duration timeout;

    public NodeProxyClient(AdminProperties properties) {
        this.timeout = Duration.ofMillis(Math.max(properties.getNodeTimeoutMs(), 200));
    }

    public AdminStatusResponse fetchStatus(ManagedNode node) throws IOException, InterruptedException {
        String url = node.getBaseUrl() + "/status";
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        ensureSuccess(response);
        AdminStatusResponse status = objectMapper.readValue(response.body(), AdminStatusResponse.class);
        status.setOnline(true);
        status.setNodeName(node.getName());
        status.setBaseUrl(node.getBaseUrl());
        if (status.getNodeId() == null || status.getNodeId().isBlank()) {
            status.setNodeId(node.getNodeId());
        }
        return status;
    }

    public List<AdminClientResponse> fetchClients(
        ManagedNode node,
        String clientId,
        String username
    ) throws IOException, InterruptedException {
        // 查询参数只在需要时拼上，避免生成冗余 URL。
        StringBuilder url = new StringBuilder(node.getBaseUrl()).append("/clients");
        boolean hasQuery = false;
        if (clientId != null && !clientId.isBlank()) {
            url.append(hasQuery ? '&' : '?').append("clientId=").append(urlEncode(clientId));
            hasQuery = true;
        }
        if (username != null && !username.isBlank()) {
            url.append(hasQuery ? '&' : '?').append("username=").append(urlEncode(username));
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(url.toString())).timeout(timeout).GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        ensureSuccess(response);
        AdminClientResponse[] items = objectMapper.readValue(response.body(), AdminClientResponse[].class);
        List<AdminClientResponse> result = Arrays.asList(items);
        for (AdminClientResponse item : result) {
            item.setNodeId(node.getNodeId());
            item.setNodeName(node.getName());
        }
        return result;
    }

    public AdminClientDetailResponse fetchClientDetail(
        ManagedNode node,
        String clientId
    ) throws IOException, InterruptedException {
        String url = node.getBaseUrl() + "/clients/" + urlEncode(clientId);
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        ensureSuccess(response);
        AdminClientDetailResponse detail = objectMapper.readValue(response.body(), AdminClientDetailResponse.class);
        detail.setNodeId(node.getNodeId());
        detail.setNodeName(node.getName());
        return detail;
    }

    public AdminStatusResponse updateConfig(
        ManagedNode node,
        AdminConfigUpdateRequest payload
    ) throws IOException, InterruptedException {
        String body = objectMapper.writeValueAsString(payload);
        HttpRequest request = HttpRequest.newBuilder(URI.create(node.getBaseUrl() + "/config"))
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        ensureSuccess(response);
        AdminStatusResponse status = objectMapper.readValue(response.body(), AdminStatusResponse.class);
        status.setOnline(true);
        status.setNodeName(node.getName());
        status.setBaseUrl(node.getBaseUrl());
        if (status.getNodeId() == null || status.getNodeId().isBlank()) {
            status.setNodeId(node.getNodeId());
        }
        return status;
    }

    private static void ensureSuccess(HttpResponse<String> response) throws IOException {
        int code = response.statusCode();
        if (code >= 200 && code < 300) {
            return;
        }
        // 统一把下游错误转成 IOException，便于上层按“节点不可用”处理。
        throw new IOException("HTTP " + code + ": " + response.body());
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
