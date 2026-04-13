package com.jmqx.store.qos;

/**
 * QoS1 下行 inflight 消息模型。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record Qos1InflightMessage(
    int packetId,
    String topic,
    byte[] payload,
    long lastSentAtMs,
    int retryCount
) {
}
