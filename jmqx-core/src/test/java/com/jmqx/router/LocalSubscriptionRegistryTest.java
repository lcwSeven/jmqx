package com.jmqx.router;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalSubscriptionRegistryTest {
    @Test
    void shouldMatchDirectAndSharedSubscribersSeparately() {
        LocalSubscriptionRegistry registry = new LocalSubscriptionRegistry();

        assertTrue(registry.subscribeAndCheckFirst("client-a", "sensor/+/status", 1));
        assertTrue(registry.subscribeAndCheckFirst("client-b", "$share/group-1/sensor/+/status", 1));
        assertFalse(registry.subscribeAndCheckFirst("client-c", "$share/group-1/sensor/+/status", 1));
        assertTrue(registry.subscribeAndCheckFirst("client-d", "$share/group-2/sensor/+/status", 1));

        SubscriptionMatchResult matchResult = registry.findSubscriptionMatch("sensor/device-1/status");

        assertEquals(Set.of("client-a"), matchResult.getDirectSubscribers());
        assertEquals(Set.of("client-b", "client-c"), matchResult.getSharedSubscribersByGroup().get("group-1"));
        assertEquals(Set.of("client-d"), matchResult.getSharedSubscribersByGroup().get("group-2"));
    }

    @Test
    void shouldTrackFirstAndLastSubscriptionForBatchUnsubscribe() {
        LocalSubscriptionRegistry registry = new LocalSubscriptionRegistry();

        assertTrue(registry.subscribeAndCheckFirst("client-a", "sensor/temp", 1));
        assertFalse(registry.subscribeAndCheckFirst("client-b", "sensor/temp", 1));
        assertTrue(registry.subscribeAndCheckFirst("client-a", "sensor/humidity", 1));

        Set<String> lastTopics = registry.unsubscribeBatchAndCollectLast(
            "client-a",
            List.of("sensor/temp", "sensor/humidity")
        );

        assertEquals(Set.of("sensor/humidity"), lastTopics);
        assertEquals(Map.of(), registry.findSubscriptions("client-a"));

        SubscriptionMatchResult temperatureMatch = registry.findSubscriptionMatch("sensor/temp");
        SubscriptionMatchResult humidityMatch = registry.findSubscriptionMatch("sensor/humidity");

        assertEquals(Set.of("client-b"), temperatureMatch.getDirectSubscribers());
        assertTrue(humidityMatch.getDirectSubscribers().isEmpty());
    }
}
