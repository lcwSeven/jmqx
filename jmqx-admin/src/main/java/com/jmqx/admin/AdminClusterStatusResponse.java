package com.jmqx.admin;

import java.util.List;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class AdminClusterStatusResponse {
    private int totalNodes;
    private int onlineNodes;
    private int totalConnections;
    private List<AdminStatusResponse> nodes;

    public int getTotalNodes() {
        return totalNodes;
    }

    public void setTotalNodes(int totalNodes) {
        this.totalNodes = totalNodes;
    }

    public int getOnlineNodes() {
        return onlineNodes;
    }

    public void setOnlineNodes(int onlineNodes) {
        this.onlineNodes = onlineNodes;
    }

    public int getTotalConnections() {
        return totalConnections;
    }

    public void setTotalConnections(int totalConnections) {
        this.totalConnections = totalConnections;
    }

    public List<AdminStatusResponse> getNodes() {
        return nodes;
    }

    public void setNodes(List<AdminStatusResponse> nodes) {
        this.nodes = nodes;
    }
}
