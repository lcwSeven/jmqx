package com.jmqx.admin;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class AdminNodeResponse {
    private String nodeId;
    private String name;
    private String baseUrl;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }
}
