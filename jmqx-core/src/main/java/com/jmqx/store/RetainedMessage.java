package com.jmqx.store;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class RetainedMessage {
    private final String topic;
    private final byte[] payload;
    private final int qos;
    private final boolean retain;

    public RetainedMessage(String topic, byte[] payload, int qos, boolean retain) {
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.retain = retain;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public int getQos() {
        return qos;
    }

    public boolean isRetain() {
        return retain;
    }
}
