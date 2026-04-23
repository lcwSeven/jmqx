package com.jmqx.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicMatcherTest {
    @Test
    void shouldMatchExactTopic() {
        assertTrue(TopicMatcher.matches("sensor/temp", "sensor/temp"));
        assertFalse(TopicMatcher.matches("sensor/temp", "sensor/humidity"));
    }

    @Test
    void shouldMatchSingleLevelWildcard() {
        assertTrue(TopicMatcher.matches("sensor/+/status", "sensor/device-1/status"));
        assertFalse(TopicMatcher.matches("sensor/+/status", "sensor/device-1/data"));
    }

    @Test
    void shouldMatchMultiLevelWildcard() {
        assertTrue(TopicMatcher.matches("sensor/#", "sensor"));
        assertTrue(TopicMatcher.matches("sensor/#", "sensor/device-1/status"));
        assertFalse(TopicMatcher.matches("sensor/#", "system/device-1/status"));
    }

    @Test
    void shouldRejectWhenTopicIsShorterThanFilter() {
        assertFalse(TopicMatcher.matches("sensor/+/status", "sensor/device-1"));
    }
}
