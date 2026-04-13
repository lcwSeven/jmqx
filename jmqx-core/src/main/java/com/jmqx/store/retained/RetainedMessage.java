package com.jmqx.store.retained;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public record RetainedMessage(String topic,
                              byte[] payload,
                              int qos,
                              boolean retain) {
}
