package com.jmqx.router;

import java.util.Map;
import java.util.Set;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface SubscriptionRegistry {
    /**
     * Subscribe and return whether this topicFilter becomes the first local subscription.
     */
    boolean subscribeAndCheckFirst(String clientId, String topicFilter, int qos);

    /**
     * Unsubscribe and return whether this topicFilter is removed from local node completely.
     */
    boolean unsubscribeAndCheckLast(String clientId, String topicFilter);

    /**
     * Remove client and collect topicFilters that become zero-ref on this node.
     */
    Set<String> removeClientAndCollectLastTopics(String clientId);

    SubscriptionMatchResult findSubscriptionMatch(String topic);

    Map<String, Integer> findSubscriptions(String clientId);
}
