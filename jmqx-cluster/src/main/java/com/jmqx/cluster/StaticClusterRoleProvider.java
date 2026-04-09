package com.jmqx.cluster;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 基于本地配置的静态角色提供器。
 * 适用于当前单进程部署与基础集群场景。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class StaticClusterRoleProvider implements ClusterRoleProvider {
    private final NodeRole role;
    private final String nodeId;
    private final Set<String> coreEndpoints;

    public StaticClusterRoleProvider(NodeRole role, String nodeId, Set<String> coreEndpoints) {
        this.role = role == null ? NodeRole.REPLICANT : role;
        this.nodeId = (nodeId == null || nodeId.isBlank()) ? "node-1" : nodeId;
        this.coreEndpoints = coreEndpoints == null
            ? Collections.emptySet()
            : Collections.unmodifiableSet(new LinkedHashSet<>(coreEndpoints));
    }

    @Override
    public NodeRole role() {
        return role;
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public Set<String> coreEndpoints() {
        return coreEndpoints;
    }
}
