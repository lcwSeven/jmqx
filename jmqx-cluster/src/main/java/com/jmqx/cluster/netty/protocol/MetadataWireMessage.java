package com.jmqx.cluster.netty.protocol;

import com.jmqx.cluster.MetadataCommand;

/**
 * 元数据网络层通用消息模型。
 * 通过 type 区分具体业务语义，字段按需使用。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public record MetadataWireMessage(
    byte type,
    long requestId,
    MetadataCommand command,
    long logIndex,
    long lastAppliedLogIndex,
    String nodeId,
    boolean success,
    String leaderEndpoint,
    String errorMessage
) {
}
