package com.jmqtt.router;

import com.jmqtt.common.TopicMatcher;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class InMemorySubscriptionRegistry implements SubscriptionRegistry {
    private final ConcurrentMap<String, ConcurrentMap<String, Integer>> subscriptionsByClient = new ConcurrentHashMap<>();

    @Override
    public void subscribe(String clientId, String topicFilter, int qos) {
        subscriptionsByClient
            .computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>())
            .put(topicFilter, qos);
    }

    @Override
    public void unsubscribe(String clientId, String topicFilter) {
        ConcurrentMap<String, Integer> clientSubscriptions = subscriptionsByClient.get(clientId);
        if (clientSubscriptions == null) {
            return;
        }
        clientSubscriptions.remove(topicFilter);
    }

    @Override
    public void removeClient(String clientId) {
        subscriptionsByClient.remove(clientId);
    }

    @Override
    public Set<String> findSubscribers(String topic) {
        Set<String> subscribers = ConcurrentHashMap.newKeySet();
        subscriptionsByClient.forEach((clientId, topics) -> {
            boolean matched = topics.keySet().stream().anyMatch(filter -> TopicMatcher.matches(filter, topic));
            if (matched) {
                subscribers.add(clientId);
            }
        });
        return subscribers;
    }

    @Override
    public Map<String, Integer> findSubscriptions(String clientId) {
        Map<String, Integer> subscriptions = subscriptionsByClient.get(clientId);
        if (subscriptions == null) {
            return Collections.emptyMap();
        }
        return Map.copyOf(subscriptions);
    }
}
