package com.jmqtt.router;

import com.jmqtt.common.SharedSubscription;
import com.jmqtt.common.TopicMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class InMemorySubscriptionRegistry implements SubscriptionRegistry {
    private final ConcurrentMap<String, ConcurrentMap<String, Integer>> subscriptionsByClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicInteger> sharedGroupIndexes = new ConcurrentHashMap<>();

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
        Set<String> directSubscribers = new LinkedHashSet<>();
        ConcurrentMap<String, List<String>> sharedSubscribersByGroup = new ConcurrentHashMap<>();

        subscriptionsByClient.forEach((clientId, topics) -> {
            topics.keySet().forEach(filter -> {
                SharedSubscription.Parsed shared = SharedSubscription.parse(filter);
                if (shared != null) {
                    if (TopicMatcher.matches(shared.topicFilter(), topic)) {
                        sharedSubscribersByGroup
                            .computeIfAbsent(shared.group(), ignored -> Collections.synchronizedList(new ArrayList<>()))
                            .add(clientId);
                    }
                    return;
                }
                if (TopicMatcher.matches(filter, topic)) {
                    directSubscribers.add(clientId);
                }
            });
        });

        Set<String> result = new LinkedHashSet<>(directSubscribers);
        sharedSubscribersByGroup.forEach((group, candidates) -> {
            String selected = pickSharedSubscriber(group, candidates);
            if (selected != null) {
                result.add(selected);
            }
        });
        return result;
    }

    @Override
    public Map<String, Integer> findSubscriptions(String clientId) {
        Map<String, Integer> subscriptions = subscriptionsByClient.get(clientId);
        if (subscriptions == null) {
            return Collections.emptyMap();
        }
        return Map.copyOf(subscriptions);
    }

    private String pickSharedSubscriber(String group, List<String> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        List<String> ordered = candidates.stream()
            .distinct()
            .sorted(Comparator.naturalOrder())
            .toList();
        if (ordered.isEmpty()) {
            return null;
        }
        AtomicInteger idx = sharedGroupIndexes.computeIfAbsent(group, ignored -> new AtomicInteger(0));
        int current = Math.floorMod(idx.getAndIncrement(), ordered.size());
        return ordered.get(current);
    }
}
