package com.jmqx.store.qos;

/**
 * QoS2 下行 inflight 消息模型。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record Qos2OutboundInflightMessage(
    int packetId,
    String topic,
    byte[] payload,
    int state,
    long lastSentAtMs
) {
}
