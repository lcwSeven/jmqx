package com.jmqx.bridge;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public interface MessageBridge extends AutoCloseable {
    MessageBridge NOOP = new MessageBridge() {
        @Override
        public void publish(BridgeMessage message) {
        }

    };

    void publish(BridgeMessage message);

    @Override
    default void close() {
    }
}
