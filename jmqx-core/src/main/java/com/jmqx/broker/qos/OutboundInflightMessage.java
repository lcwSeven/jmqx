package com.jmqx.broker.qos;

import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * 统一的出站 inflight 消息模型。
 * 同时承载 QoS1 与 QoS2 的状态信息，降低处理分支重复。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record OutboundInflightMessage(
    int qos,
    String topic,
    byte[] payload,
    Qos2State qos2State,
    long lastSentAtMs,
    int retryCount
) {
    public static OutboundInflightMessage qos1(String topic, byte[] payload, long sentAtMs, int retryCount) {
        return new OutboundInflightMessage(MqttQoS.AT_LEAST_ONCE.value(), topic, payload, null, sentAtMs, retryCount);
    }

    public static OutboundInflightMessage qos2WaitPubRec(String topic, byte[] payload, long sentAtMs) {
        return new OutboundInflightMessage(MqttQoS.EXACTLY_ONCE.value(), topic, payload, Qos2State.WAIT_PUBREC, sentAtMs, 0);
    }

    public static OutboundInflightMessage qos2(String topic, byte[] payload, Qos2State state, long sentAtMs) {
        return new OutboundInflightMessage(MqttQoS.EXACTLY_ONCE.value(), topic, payload, state, sentAtMs, 0);
    }

    public OutboundInflightMessage toQos2WaitPubComp(long sentAtMs) {
        return new OutboundInflightMessage(qos, topic, payload, Qos2State.WAIT_PUBCOMP, sentAtMs, retryCount);
    }

    public OutboundInflightMessage nextQos1Retry(long sentAtMs) {
        return new OutboundInflightMessage(qos, topic, payload, qos2State, sentAtMs, retryCount + 1);
    }

    public OutboundInflightMessage nextQos2Retry(long sentAtMs) {
        return new OutboundInflightMessage(qos, topic, payload, qos2State, sentAtMs, retryCount + 1);
    }
}
