package com.jmqx.cluster;

import java.util.UUID;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ClusterPublishMessage {
    private final String messageId;
    private final String sourceNodeId;
    private final String topic;
    private final byte[] payload;
    private final int qos;
    private final boolean retain;

    public ClusterPublishMessage(String sourceNodeId, String topic, byte[] payload, int qos, boolean retain) {
        this.messageId = UUID.randomUUID().toString();
        this.sourceNodeId = sourceNodeId;
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.retain = retain;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
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
