package com.jmqx.router;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface SubscriptionRegistry {
    /**
     * Subscribe and return whether this topicFilter becomes the first local subscription.
     */
    boolean subscribeAndCheckFirst(String clientId, String topicFilter, int qos);

    /**
     * Unsubscribe and return whether this topicFilter is removed from local node completely.
     */
    boolean unsubscribeAndCheckLast(String clientId, String topicFilter);

    /**
     * 批量退订并返回本节点引用计数降为 0 的 topicFilter 集合。
     * 默认实现为逐条调用，具体实现可重写为单次加锁批处理。
     */
    default Set<String> unsubscribeBatchAndCollectLast(String clientId, List<String> topicFilters) {
        if (topicFilters == null || topicFilters.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> lastTopics = new java.util.LinkedHashSet<>();
        for (String topicFilter : topicFilters) {
            if (unsubscribeAndCheckLast(clientId, topicFilter)) {
                lastTopics.add(topicFilter);
            }
        }
        return lastTopics;
    }

    /**
     * Remove client and collect topicFilters that become zero-ref on this node.
     */
    Set<String> removeClientAndCollectLastTopics(String clientId);

    SubscriptionMatchResult findSubscriptionMatch(String topic);

    Map<String, Integer> findSubscriptions(String clientId);
}
