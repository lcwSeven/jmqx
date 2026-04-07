package com.jmqx.admin;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminProperties {
    private boolean enabled = true;
    private String host = "0.0.0.0";
    private int port = 18083;
    private String nodes = "local=http://127.0.0.1:28083/api/admin";
    private int nodeTimeoutMs = 2000;
    private boolean frontendIntegrated = true;
    private boolean frontendBuildOnStart = true;
    private String frontendBuildWorkDir = "jmqx-admin/frontend";
    private String frontendBuildCommand = "npm run build";
    private String frontendDistDir = "jmqx-admin/frontend/dist";

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

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public int getNodeTimeoutMs() {
        return nodeTimeoutMs;
    }

    public void setNodeTimeoutMs(int nodeTimeoutMs) {
        this.nodeTimeoutMs = nodeTimeoutMs;
    }

    public boolean isFrontendIntegrated() {
        return frontendIntegrated;
    }

    public void setFrontendIntegrated(boolean frontendIntegrated) {
        this.frontendIntegrated = frontendIntegrated;
    }

    public boolean isFrontendBuildOnStart() {
        return frontendBuildOnStart;
    }

    public void setFrontendBuildOnStart(boolean frontendBuildOnStart) {
        this.frontendBuildOnStart = frontendBuildOnStart;
    }

    public String getFrontendBuildWorkDir() {
        return frontendBuildWorkDir;
    }

    public void setFrontendBuildWorkDir(String frontendBuildWorkDir) {
        this.frontendBuildWorkDir = frontendBuildWorkDir;
    }

    public String getFrontendBuildCommand() {
        return frontendBuildCommand;
    }

    public void setFrontendBuildCommand(String frontendBuildCommand) {
        this.frontendBuildCommand = frontendBuildCommand;
    }

    public String getFrontendDistDir() {
        return frontendDistDir;
    }

    public void setFrontendDistDir(String frontendDistDir) {
        this.frontendDistDir = frontendDistDir;
    }
}
