package com.jmqx.admin;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class ManagedNode {
    private final String nodeId;
    private final String name;
    private final String baseUrl;

    public ManagedNode(String nodeId, String name, String baseUrl) {
        this.nodeId = nodeId;
        this.name = name;
        this.baseUrl = baseUrl;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getName() {
        return name;
    }

    public String getBaseUrl() {
        return baseUrl;
    }
}
