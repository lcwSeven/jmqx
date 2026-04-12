package com.jmqx.cluster.netty;

import java.util.Objects;

/**
 * 集群节点地址模型。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public record ClusterEndpoint(String host, int port) {
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ClusterEndpoint that)) {
            return false;
        }
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    public static ClusterEndpoint parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String value = raw.trim();
        int idx = value.lastIndexOf(':');
        if (idx <= 0 || idx >= value.length() - 1) {
            return null;
        }
        try {
            String host = value.substring(0, idx);
            int port = Integer.parseInt(value.substring(idx + 1));
            if (port <= 0) {
                return null;
            }
            return new ClusterEndpoint(host, port);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

}
