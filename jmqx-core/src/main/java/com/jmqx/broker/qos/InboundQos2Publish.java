package com.jmqx.broker.qos;

/**
 * QoS2 上行阶段暂存消息（等待 PUBREL）。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record InboundQos2Publish(String topic, byte[] payload, boolean retain) {
}
