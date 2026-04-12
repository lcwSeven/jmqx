package com.jmqx.cluster;

import java.util.Set;

/**
 * 集群节点角色与身份提供器。
 * 用于统一获取节点角色、节点 ID 和 Core 节点地址列表。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public interface ClusterRoleProvider {

    NodeRole role();

    String nodeId();

    Set<String> coreEndpoints();
}
