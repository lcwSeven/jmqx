package com.jmqx.admin;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * 独立 admin 的聚合入口。
 * 负责节点管理、集群状态聚合、客户端查询以及配置透传。
 *
 * @author liucaiwen
 * @date 2026/4/5
 */
@RestController
@RequestMapping("/api/admin")
@CrossOrigin(origins = "*")
public class AdminController {
    private final NodeRegistryService nodeRegistryService;
    private final NodeProxyClient nodeProxyClient;

    public AdminController(NodeRegistryService nodeRegistryService, NodeProxyClient nodeProxyClient) {
        this.nodeRegistryService = nodeRegistryService;
        this.nodeProxyClient = nodeProxyClient;
    }

    @GetMapping("/status")
    public AdminStatusResponse status(@RequestParam(value = "nodeId", required = false) String nodeId) {
        ManagedNode node = resolveNode(nodeId);
        if (node == null) {
            AdminStatusResponse empty = new AdminStatusResponse();
            empty.setOnline(false);
            empty.setErrorMessage("no managed node configured");
            return empty;
        }
        try {
            return nodeProxyClient.fetchStatus(node);
        } catch (IOException | InterruptedException e) {
            return offlineNode(node, e);
        }
    }

    @GetMapping("/cluster/status")
    public AdminClusterStatusResponse clusterStatus() {
        List<ManagedNode> nodes = nodeRegistryService.list();
        List<AdminStatusResponse> statuses = new ArrayList<>();
        int online = 0;
        int totalConnections = 0;
        // 这里逐个拉取节点状态并做轻量聚合，保持控制面实现简单直接。
        for (ManagedNode node : nodes) {
            try {
                AdminStatusResponse status = nodeProxyClient.fetchStatus(node);
                statuses.add(status);
                if (status.isOnline()) {
                    online++;
                    totalConnections += status.getConnections();
                }
            } catch (IOException | InterruptedException e) {
                statuses.add(offlineNode(node, e));
            }
        }
        AdminClusterStatusResponse response = new AdminClusterStatusResponse();
        response.setTotalNodes(nodes.size());
        response.setOnlineNodes(online);
        response.setTotalConnections(totalConnections);
        response.setNodes(statuses);
        return response;
    }

    @GetMapping("/nodes")
    public List<AdminNodeResponse> nodes() {
        return nodeRegistryService.list().stream().map(this::toNodeResponse).toList();
    }

    @PostMapping("/nodes")
    public AdminNodeResponse addNode(@RequestBody AdminNodeRequest request) {
        if (request == null || request.getBaseUrl() == null || request.getBaseUrl().isBlank()) {
            throw new ResponseStatusException(BAD_REQUEST, "baseUrl is required");
        }
        ManagedNode node = nodeRegistryService.add(request.getName(), request.getBaseUrl());
        return toNodeResponse(node);
    }

    @DeleteMapping("/nodes/{nodeId}")
    public void removeNode(@PathVariable("nodeId") String nodeId) {
        if (!nodeRegistryService.remove(nodeId)) {
            throw new ResponseStatusException(NOT_FOUND, "node not found: " + nodeId);
        }
    }

    @GetMapping("/clients")
    public List<AdminClientResponse> clients(
        @RequestParam(value = "nodeId", required = false) String nodeId,
        @RequestParam(value = "clientId", required = false) String clientId,
        @RequestParam(value = "username", required = false) String username
    ) {
        String clientIdQuery = normalize(clientId);
        String usernameQuery = normalize(username);
        List<ManagedNode> targets = resolveNodes(nodeId);
        List<AdminClientResponse> result = new ArrayList<>();
        // 客户端列表允许按节点聚合查询，节点离线时直接跳过，避免影响整体结果。
        for (ManagedNode node : targets) {
            try {
                result.addAll(nodeProxyClient.fetchClients(node, clientIdQuery, usernameQuery));
            } catch (IOException | InterruptedException ignored) {
            }
        }
        result.sort(Comparator.comparingLong(AdminClientResponse::getOnlineAtEpochMillis).reversed());
        return result;
    }

    @GetMapping("/clients/{clientId}")
    public AdminClientDetailResponse clientDetail(
        @PathVariable("clientId") String clientId,
        @RequestParam(value = "nodeId", required = false) String nodeId
    ) {
        List<ManagedNode> targets = resolveNodes(nodeId);
        for (ManagedNode node : targets) {
            try {
                return nodeProxyClient.fetchClientDetail(node, clientId);
            } catch (IOException | InterruptedException ignored) {
            }
        }
        throw new ResponseStatusException(NOT_FOUND, "client not found: " + clientId);
    }

    @PostMapping("/config")
    public AdminStatusResponse updateConfig(
        @RequestBody AdminConfigUpdateRequest request,
        @RequestParam(value = "nodeId", required = false) String nodeId
    ) {
        ManagedNode node = resolveNode(nodeId);
        if (node == null) {
            throw new ResponseStatusException(BAD_REQUEST, "no target node");
        }
        try {
            // 配置变更不在 admin 本地落地，而是原样透传到目标 broker 节点热更新。
            return nodeProxyClient.updateConfig(node, request);
        } catch (IOException | InterruptedException e) {
            throw new ResponseStatusException(BAD_REQUEST, "update node config failed: " + e.getMessage());
        }
    }

    private AdminStatusResponse offlineNode(ManagedNode node, Exception exception) {
        AdminStatusResponse status = new AdminStatusResponse();
        status.setNodeId(node.getNodeId());
        status.setNodeName(node.getName());
        status.setBaseUrl(node.getBaseUrl());
        status.setOnline(false);
        status.setErrorMessage(exception.getMessage());
        return status;
    }

    private ManagedNode resolveNode(String nodeId) {
        ManagedNode node = nodeRegistryService.get(nodeId);
        return node != null ? node : nodeRegistryService.first();
    }

    private List<ManagedNode> resolveNodes(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return nodeRegistryService.list();
        }
        ManagedNode node = nodeRegistryService.get(nodeId);
        if (node == null) {
            return List.of();
        }
        return List.of(node);
    }

    private AdminNodeResponse toNodeResponse(ManagedNode node) {
        AdminNodeResponse response = new AdminNodeResponse();
        response.setNodeId(node.getNodeId());
        response.setName(node.getName());
        response.setBaseUrl(node.getBaseUrl());
        return response;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }
}
