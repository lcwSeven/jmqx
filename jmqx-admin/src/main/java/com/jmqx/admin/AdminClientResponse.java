package com.jmqx.admin;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminClientResponse {
    private String clientId;
    private long onlineAtEpochMillis;
    private String username;
    private String connectionType;
    private String serviceNodeIp;
    private int keepAliveSeconds;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public long getOnlineAtEpochMillis() {
        return onlineAtEpochMillis;
    }

    public void setOnlineAtEpochMillis(long onlineAtEpochMillis) {
        this.onlineAtEpochMillis = onlineAtEpochMillis;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(String connectionType) {
        this.connectionType = connectionType;
    }

    public String getServiceNodeIp() {
        return serviceNodeIp;
    }

    public void setServiceNodeIp(String serviceNodeIp) {
        this.serviceNodeIp = serviceNodeIp;
    }

    public int getKeepAliveSeconds() {
        return keepAliveSeconds;
    }

    public void setKeepAliveSeconds(int keepAliveSeconds) {
        this.keepAliveSeconds = keepAliveSeconds;
    }
}
