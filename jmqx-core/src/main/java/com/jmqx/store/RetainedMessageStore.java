package com.jmqx.store;

import java.util.List;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface RetainedMessageStore {
    void saveOrRemove(RetainedMessage message);

    List<RetainedMessage> findByTopicFilter(String topicFilter);

    default RetainedStoreMetrics metrics() {
        return RetainedStoreMetrics.EMPTY;
    }

    default void close() {
    }
}
