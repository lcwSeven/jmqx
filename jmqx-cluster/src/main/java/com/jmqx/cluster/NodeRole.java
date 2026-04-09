package com.jmqx.cluster;

/**
 * 集群节点角色定义。
 * CORE 负责强一致元数据写入；
 * REPLICANT 负责承接连接并维护本地只读副本。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public enum NodeRole {
    CORE,
    REPLICANT;

    public static NodeRole from(String raw, NodeRole defaultRole) {
        if (raw == null || raw.isBlank()) {
            return defaultRole;
        }
        for (NodeRole value : values()) {
            if (value.name().equalsIgnoreCase(raw)) {
                return value;
            }
        }
        return defaultRole;
    }
}
