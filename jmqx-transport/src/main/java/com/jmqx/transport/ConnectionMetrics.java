package com.jmqx.transport;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ConnectionMetrics {
    private final AtomicInteger activeConnections = new AtomicInteger();

    public void onConnected() {
        activeConnections.incrementAndGet();
    }

    public void onDisconnected() {
        activeConnections.updateAndGet(v -> Math.max(v - 1, 0));
    }

    public int getActiveConnections() {
        return activeConnections.get();
    }
}
