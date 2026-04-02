package com.jmqtt.router;

import java.util.Map;
import java.util.Set;

public interface SubscriptionRegistry {
    void subscribe(String clientId, String topicFilter, int qos);

    void unsubscribe(String clientId, String topicFilter);

    void removeClient(String clientId);

    Set<String> findSubscribers(String topic);

    Map<String, Integer> findSubscriptions(String clientId);
}
