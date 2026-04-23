package com.jmqx.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class SharedSubscriptionTest {
    @Test
    void shouldParseSharedSubscriptionFilter() {
        SharedSubscription.Parsed parsed = SharedSubscription.parse("$share/group-a/sensor/+/status");

        assertNotNull(parsed);
        assertEquals("group-a", parsed.group());
        assertEquals("sensor/+/status", parsed.topicFilter());
    }

    @Test
    void shouldRejectInvalidSharedSubscriptionFilter() {
        assertNull(SharedSubscription.parse("sensor/+/status"));
        assertNull(SharedSubscription.parse("$share//sensor/+/status"));
        assertNull(SharedSubscription.parse("$share/group-a/"));
    }

    @Test
    void shouldNormalizeToRealTopicFilter() {
        assertEquals(
            "sensor/+/status",
            SharedSubscription.normalizeTopicFilter("$share/group-a/sensor/+/status")
        );
        assertEquals("sensor/+/status", SharedSubscription.normalizeTopicFilter("sensor/+/status"));
    }
}
