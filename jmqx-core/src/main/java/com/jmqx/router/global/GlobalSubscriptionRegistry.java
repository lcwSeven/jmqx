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

    void clear();

    GlobalSubscriptionMatch match(String topic);

    Map<String, Set<String>> snapshotNodeToTopicKeys();

    long appliedLogIndex();
}
