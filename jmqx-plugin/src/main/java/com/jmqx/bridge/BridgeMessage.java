package com.jmqx.bridge;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public record BridgeMessage(String clientId,
                            String topic,
                            byte[] payload,
                            int qos,
                            boolean retain,
                            long publishedAt) {
}
