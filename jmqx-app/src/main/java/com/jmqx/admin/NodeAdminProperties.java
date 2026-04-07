package com.jmqx.admin;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class NodeAdminProperties {
    private boolean enabled = true;
    private String host = "0.0.0.0";
    private int port = 28083;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
