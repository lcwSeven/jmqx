package com.jmqtt.store;

import com.jmqtt.common.TopicMatcher;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class InMemoryRetainedMessageStore implements RetainedMessageStore {
    private final ConcurrentMap<String, RetainedMessage> retainedMessages = new ConcurrentHashMap<>();

    @Override
    public void saveOrRemove(RetainedMessage message) {
        if (message.getPayload().length == 0) {
            retainedMessages.remove(message.getTopic());
            return;
        }
        retainedMessages.put(message.getTopic(), message);
    }

    @Override
    public List<RetainedMessage> findByTopicFilter(String topicFilter) {
        return retainedMessages
            .values()
            .stream()
            .filter(message -> TopicMatcher.matches(topicFilter, message.getTopic()))
            .toList();
    }
}
