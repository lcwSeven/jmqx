package com.jmqtt.cluster;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public interface ClusterReplicator {
    void replicatePublish(String topic, byte[] payload, int qos, boolean retain);
}
