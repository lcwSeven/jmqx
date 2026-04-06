package com.jmqx.cluster;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class NoopClusterReplicator implements ClusterReplicator {
    @Override
    public void replicatePublish(String topic, byte[] payload, int qos, boolean retain) {
    }
}
