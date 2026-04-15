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
    boolean retain,
    byte state
) {
    public Qos2InboundInflightMessage {
        state = Qos2InboundState.fromCode(state).code();
    }

    public static Qos2InboundInflightMessage waitingPubRel(
        int packetId,
        String topic,
        byte[] payload,
        boolean retain
    ) {
        return new Qos2InboundInflightMessage(packetId, topic, payload, retain, Qos2InboundState.WAIT_PUBREL.code());
    }

    public Qos2InboundState inboundState() {
        return Qos2InboundState.fromCode(state);
    }

    public Qos2InboundInflightMessage toCompleted() {
        return new Qos2InboundInflightMessage(packetId, topic, payload, retain, Qos2InboundState.COMPLETED.code());
    }
}
