package com.jmqx.admin;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * admin 进程内的节点注册表。
 * 当前使用内存存储，适合单实例控制台；后续如果做高可用可以替换成数据库或配置中心。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
@Service
public class NodeRegistryService {
    private final ConcurrentMap<String, ManagedNode> nodes = new ConcurrentHashMap<>();

    public NodeRegistryService(AdminProperties properties) {
        loadDefaults(properties.getNodes());
    }

    public List<ManagedNode> list() {
        return new ArrayList<>(nodes.values());
    }

    public ManagedNode get(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return null;
        }
        return nodes.get(nodeId.trim());
    }

    public ManagedNode first() {
        return nodes.values().stream().findFirst().orElse(null);
    }

    public ManagedNode add(String name, String baseUrl) {
        String normalizedName = normalizeName(name);
        String normalizedBaseUrl = normalizeBaseUrl(baseUrl);
        String nodeId = buildNodeId(normalizedName);
        ManagedNode node = new ManagedNode(nodeId, normalizedName, normalizedBaseUrl);
        nodes.put(nodeId, node);
        return node;
    }

    public boolean remove(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return false;
        }
        return nodes.remove(nodeId.trim()) != null;
    }

    private void loadDefaults(String raw) {
        if (raw == null || raw.isBlank()) {
            return;
        }
        // 约定格式为 name=url,name=url，方便通过 properties 快速预置节点。
        String[] parts = raw.split(",");
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            int idx = part.indexOf('=');
            if (idx < 0) {
                add("node-" + (nodes.size() + 1), part.trim());
                continue;
            }
            String name = part.substring(0, idx).trim();
            String baseUrl = part.substring(idx + 1).trim();
            add(name, baseUrl);
        }
    }

    private static String normalizeName(String name) {
        if (name == null || name.isBlank()) {
            return "node";
        }
        return name.trim();
    }

    private static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("baseUrl is required");
        }
        String result = baseUrl.trim();
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private String buildNodeId(String name) {
        String cleaned = name.toLowerCase().replaceAll("[^a-z0-9]+", "-");
        cleaned = cleaned.replaceAll("^-+|-+$", "");
        if (cleaned.isBlank()) {
            cleaned = "node";
        }
        String candidate = cleaned;
        if (!nodes.containsKey(candidate)) {
            return candidate;
        }
        // 名称重复时自动补随机后缀，避免用户手工维护唯一 ID。
        while (true) {
            String next = candidate + "-" + UUID.randomUUID().toString().substring(0, 6);
            if (!nodes.containsKey(next)) {
                return next;
            }
        }
    }
}
