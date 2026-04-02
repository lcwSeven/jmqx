/**
 * @author liucaiwen
 * @date 2026/4/2
 */
package com.jmqtt.common;

import java.util.Objects;

public final class TopicMatcher {
    private TopicMatcher() {
    }

    public static boolean matches(String filter, String topic) {
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(topic, "topic");

        String[] filterLevels = filter.split("/", -1);
        String[] topicLevels = topic.split("/", -1);

        int i = 0;
        for (; i < filterLevels.length; i++) {
            String current = filterLevels[i];
            if ("#".equals(current)) {
                return i == filterLevels.length - 1;
            }
            if (i >= topicLevels.length) {
                return false;
            }
            if ("+".equals(current)) {
                continue;
            }
            if (!current.equals(topicLevels[i])) {
                return false;
            }
        }
        return i == topicLevels.length;
    }
}
