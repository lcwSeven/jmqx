package com.jmqx.bridge;

import java.util.ArrayList;
import java.util.List;

/**
 * 仅在消息主题命中过滤器时才转发给下游桥接器。
 *
 * @author liucaiwen
 * @date 2026/4/15
 */
public class FilteredMessageBridge implements MessageBridge {
    private final MessageBridge delegate;
    private final List<String> filters;

    public FilteredMessageBridge(MessageBridge delegate, List<String> filters) {
        this.delegate = delegate;
        this.filters = new ArrayList<>(filters);
    }

    @Override
    public void publish(BridgeMessage message) {
        String topic = message == null ? null : message.topic();
        if (topic == null || topic.isBlank()) {
            return;
        }
        for (String filter : filters) {
            if (matchesTopicFilter(filter, topic)) {
                delegate.publish(message);
                return;
            }
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static boolean matchesTopicFilter(String filter, String topic) {
        if (filter == null || filter.isBlank() || topic == null || topic.isBlank()) {
            return false;
        }
        String[] filterLevels = filter.split("/", -1);
        String[] topicLevels = topic.split("/", -1);
        int filterIndex = 0;
        int topicIndex = 0;

        while (filterIndex < filterLevels.length && topicIndex < topicLevels.length) {
            String currentFilterLevel = filterLevels[filterIndex];
            String currentTopicLevel = topicLevels[topicIndex];

            if ("#".equals(currentFilterLevel)) {
                return filterIndex == filterLevels.length - 1;
            }
            if ("+".equals(currentFilterLevel)) {
                filterIndex++;
                topicIndex++;
                continue;
            }
            if (!currentFilterLevel.equals(currentTopicLevel)) {
                return false;
            }
            filterIndex++;
            topicIndex++;
        }

        if (filterIndex == filterLevels.length && topicIndex == topicLevels.length) {
            return true;
        }
        return filterIndex == filterLevels.length - 1 && "#".equals(filterLevels[filterIndex]);
    }
}
