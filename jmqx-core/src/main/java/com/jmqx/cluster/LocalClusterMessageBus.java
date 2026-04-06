package com.jmqx.cluster;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class LocalClusterMessageBus implements ClusterMessageBus {
    private final List<ClusterMessageListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        listeners.clear();
    }

    @Override
    public void publish(ClusterPublishMessage message) {
        for (ClusterMessageListener listener : listeners) {
            listener.onPublish(message);
        }
    }

    @Override
    public void registerListener(ClusterMessageListener listener) {
        listeners.add(listener);
    }
}
