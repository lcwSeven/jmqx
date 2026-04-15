package com.jmqx.bridge;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class BridgeMessage {
    private final String clientId;
    private final String topic;
    private final byte[] payload;
    private final int qos;
    private final boolean retain;
    private final long publishedAt;

    public BridgeMessage(String clientId, String topic, byte[] payload, int qos, boolean retain, long publishedAt) {
        this.clientId = clientId;
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.retain = retain;
        this.publishedAt = publishedAt;
    }

    public String clientId() {
        return clientId;
    }

    public String topic() {
        return topic;
    }

    public byte[] payload() {
        return payload;
    }

    public int qos() {
        return qos;
    }

    public boolean retain() {
        return retain;
    }

    public long publishedAt() {
        return publishedAt;
    }
}
