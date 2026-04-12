package com.jmqx.broker.core;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Cluster dispatcher for forwarding publish payload to remote nodes.
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
@FunctionalInterface
public interface ClusterMessageDispatcher {
    /**
     * Dispatch the payload to remote nodes.
     *
     * @param topic       topic
     * @param payload     payload
     * @param publishQos  publish qos
     * @param targetPlans target plans
     */
    void dispatch(String topic, byte[] payload, int publishQos, Map<String, DispatchTarget> targetPlans);

    /**
     * Dispatch target.  转发目标
     */
    record DispatchTarget(boolean includeNormal, Set<String> sharedGroups) {
        public DispatchTarget {
            sharedGroups = sharedGroups == null ? Collections.emptySet() : Collections.unmodifiableSet(new LinkedHashSet<>(sharedGroups));
        }

        public static DispatchTarget normalOnly() {
            return new DispatchTarget(true, Collections.emptySet());
        }

        public static DispatchTarget sharedOnly(Set<String> sharedGroups) {
            return new DispatchTarget(false, sharedGroups);
        }

        public static DispatchTarget normalAndShared(Set<String> sharedGroups) {
            return new DispatchTarget(true, sharedGroups);
        }
    }
}
