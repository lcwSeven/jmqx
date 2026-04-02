package com.jmqtt.store;

import java.util.List;

public interface RetainedMessageStore {
    void saveOrRemove(RetainedMessage message);

    List<RetainedMessage> findByTopicFilter(String topicFilter);
}
