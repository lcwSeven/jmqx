package com.jmqx.bridge;

import java.util.List;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class MultiMessageBridge implements MessageBridge {
    private static final Logger LOG = Logger.getLogger(MultiMessageBridge.class.getName());
    private final List<MessageBridge> delegates;

    public MultiMessageBridge(List<MessageBridge> delegates) {
        this.delegates = delegates;
    }

    @Override
    public void publish(BridgeMessage message) {
        for (MessageBridge delegate : delegates) {
            try {
                delegate.publish(message);
            } catch (Exception e) {
                LOG.warning("[BRIDGE] delegate publish failed: " + e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        for (MessageBridge delegate : delegates) {
            try {
                delegate.close();
            } catch (Exception ignored) {
            }
        }
    }
}
