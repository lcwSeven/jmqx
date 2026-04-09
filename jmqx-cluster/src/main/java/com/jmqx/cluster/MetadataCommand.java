package com.jmqx.cluster;

/**
 * 元数据写命令模型。
 * REPLICANT 节点会把订阅路由等元数据操作封装为该命令发送给 CORE。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public record MetadataCommand(
    String namespace,
    String operation,
    String key,
    String value,
    String sourceNodeId
) {
}
