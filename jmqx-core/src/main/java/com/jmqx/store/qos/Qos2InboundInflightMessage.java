package com.jmqx.store.qos;

/**
 * QoS2 上行 inflight 消息模型。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record Qos2InboundInflightMessage(
    int packetId,
    String topic,
    byte[] payload,
    boolean retain
) {
}
