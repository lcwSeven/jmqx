package com.jmqx.broker;

import java.util.Set;

/**
 * Cluster dispatcher for forwarding publish payload to remote nodes.
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
@FunctionalInterface
public interface ClusterMessageDispatcher {
    ClusterMessageDispatcher NOOP = (topic, payload, targetNodeIds) -> {
    };

    void dispatch(String topic, byte[] payload, Set<String> targetNodeIds);
}
