package com.jmqx.admin;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminStatusResponse {
    private int connections;
    private String authType;
    private int authCacheMillis;
    private String aclType;
    private int aclCacheMillis;

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
