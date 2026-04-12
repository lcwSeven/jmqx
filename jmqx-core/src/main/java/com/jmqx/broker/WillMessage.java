package com.jmqx.broker;

/**
 * 连接建立时缓存的遗嘱消息。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record WillMessage(String topic, byte[] payload, int qos, boolean retain) {
}
