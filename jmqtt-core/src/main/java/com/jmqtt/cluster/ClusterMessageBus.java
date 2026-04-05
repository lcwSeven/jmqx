package com.jmqtt.cluster;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public interface ClusterMessageBus {
    void start() throws Exception;

    void stop();

    void publish(ClusterPublishMessage message);

    void registerListener(ClusterMessageListener listener);
}
