package com.jmqx.cluster;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public interface ClusterMessageListener {
    void onPublish(ClusterPublishMessage message);
}
