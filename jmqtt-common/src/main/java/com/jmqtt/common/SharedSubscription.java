package com.jmqtt.common;

/**
 * @author liucaiwen
 * @date 2026/4/6
 */
public final class SharedSubscription {
    private static final String PREFIX = "$share/";

    private SharedSubscription() {
    }

    public static Parsed parse(String topicFilter) {
        if (topicFilter == null || !topicFilter.startsWith(PREFIX)) {
            return null;
        }
        String remaining = topicFilter.substring(PREFIX.length());
        int split = remaining.indexOf('/');
        if (split <= 0 || split == remaining.length() - 1) {
            return null;
        }
        String group = remaining.substring(0, split);
        String realFilter = remaining.substring(split + 1);
        if (group.isBlank() || realFilter.isBlank()) {
            return null;
        }
        return new Parsed(group, realFilter);
    }

    public static String normalizeTopicFilter(String topicFilter) {
        Parsed parsed = parse(topicFilter);
        if (parsed == null) {
            return topicFilter;
        }
        return parsed.topicFilter();
    }

    /**
     * @author liucaiwen
     * @date 2026/4/6
     */
    public record Parsed(String group, String topicFilter) {
    }
}
