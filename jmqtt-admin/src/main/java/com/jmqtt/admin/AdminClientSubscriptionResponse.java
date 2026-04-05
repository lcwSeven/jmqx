package com.jmqtt.admin;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminClientSubscriptionResponse {
    private String topic;
    private int qos;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }
}
