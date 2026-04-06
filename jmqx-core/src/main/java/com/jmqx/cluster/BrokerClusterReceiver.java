package com.jmqx.cluster;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public interface BrokerClusterReceiver {
    void onClusterPublish(ClusterPublishMessage message);
}
