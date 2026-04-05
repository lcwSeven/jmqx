package com.jmqtt.cluster;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ReloadableClusterReplicator implements ClusterReplicator {
    private volatile ClusterReplicator delegate;

    public ReloadableClusterReplicator(ClusterReplicator delegate) {
        this.delegate = delegate;
    }

    public void setDelegate(ClusterReplicator delegate) {
        this.delegate = delegate;
    }

    @Override
    public void replicatePublish(String topic, byte[] payload, int qos, boolean retain) {
        delegate.replicatePublish(topic, payload, qos, retain);
    }
}
