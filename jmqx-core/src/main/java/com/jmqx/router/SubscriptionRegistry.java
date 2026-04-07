package com.jmqx.router;

import java.util.Map;
import java.util.Set;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface SubscriptionRegistry {
    void subscribe(String clientId, String topicFilter, int qos);

    void unsubscribe(String clientId, String topicFilter);

    void removeClient(String clientId);

    Set<String> findSubscribers(String topic);

    SubscriptionMatchResult findSubscriptionMatch(String topic);

    Map<String, Integer> findSubscriptions(String clientId);
}
