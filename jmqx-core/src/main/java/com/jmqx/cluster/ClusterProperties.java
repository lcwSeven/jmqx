package com.jmqx.cluster;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ClusterProperties {
    private boolean enabled = false;
    private String nodeId = "node-1";
    private ClusterRole role = ClusterRole.MASTER;
    private String busType = "local";
    private String seedNodes = "";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public ClusterRole getRole() {
        return role;
    }

    public void setRole(ClusterRole role) {
        this.role = role;
    }

    public String getBusType() {
        return busType;
    }

    public void setBusType(String busType) {
        this.busType = busType;
    }

    public String getSeedNodes() {
        return seedNodes;
    }

    public void setSeedNodes(String seedNodes) {
        this.seedNodes = seedNodes;
    }
}
