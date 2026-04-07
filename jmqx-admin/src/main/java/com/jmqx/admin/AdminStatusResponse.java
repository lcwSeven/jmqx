package com.jmqx.admin;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminStatusResponse {
    private String nodeId;
    private String nodeName;
    private String baseUrl;
    private boolean online;
    private String errorMessage;
    private int connections;
    private String authType;
    private int authCacheMillis;
    private String aclType;
    private int aclCacheMillis;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public int getConnections() {
        return connections;
    }

    public void setConnections(int connections) {
        this.connections = connections;
    }

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    public int getAuthCacheMillis() {
        return authCacheMillis;
    }

    public void setAuthCacheMillis(int authCacheMillis) {
        this.authCacheMillis = authCacheMillis;
    }

    public String getAclType() {
        return aclType;
    }

    public void setAclType(String aclType) {
        this.aclType = aclType;
    }

    public int getAclCacheMillis() {
        return aclCacheMillis;
    }

    public void setAclCacheMillis(int aclCacheMillis) {
        this.aclCacheMillis = aclCacheMillis;
    }
}
