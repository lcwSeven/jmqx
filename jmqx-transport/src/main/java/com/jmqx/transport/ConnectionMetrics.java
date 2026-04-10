package com.jmqx.transport;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ConnectionMetrics {
    private final AtomicInteger activeConnections = new AtomicInteger();
    private final AtomicLong inboundBytes = new AtomicLong();
    private final AtomicLong outboundBytes = new AtomicLong();

    public void onConnected() {
        activeConnections.incrementAndGet();
    }

    public void onDisconnected() {
        activeConnections.updateAndGet(v -> Math.max(v - 1, 0));
    }

    public int getActiveConnections() {
        return activeConnections.get();
    }

    public void addInboundBytes(long bytes) {
        if (bytes <= 0) {
            return;
        }
        inboundBytes.addAndGet(bytes);
    }

    public void addOutboundBytes(long bytes) {
        if (bytes <= 0) {
            return;
        }
        outboundBytes.addAndGet(bytes);
    }

    public long getInboundBytes() {
        return inboundBytes.get();
    }

    public long getOutboundBytes() {
        return outboundBytes.get();
    }
}
