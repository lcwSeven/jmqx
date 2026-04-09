package com.jmqx.router.global;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Global subscription registry.
 *
 * Core writes subscription events into log, all nodes apply events into this registry.
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public interface GlobalSubscriptionRegistry {
    void apply(GlobalSubscriptionEvent event);

    default void applyBatch(List<GlobalSubscriptionEvent> events) {
        if (events == null) {
            return;
        }
        for (GlobalSubscriptionEvent event : events) {
            apply(event);
        }
    }

    GlobalSubscriptionMatch match(String topic);

    List<GlobalSubscriptionEvent> buildNodeDownCleanupEvents(String nodeId, long startLogIndexExclusive);

    Set<String> getNodeTopicKeys(String nodeId);

    Map<String, Set<String>> snapshotNodeToTopicKeys();

    long appliedLogIndex();
}
