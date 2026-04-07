package com.jmqx.router;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * 订阅匹配结果：普通订阅与共享订阅候选分开返回。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class SubscriptionMatchResult {
    private final Set<String> directSubscribers;
    private final Map<String, Set<String>> sharedSubscribersByGroup;

    public SubscriptionMatchResult(Set<String> directSubscribers, Map<String, Set<String>> sharedSubscribersByGroup) {
        this.directSubscribers = directSubscribers == null ? Collections.emptySet() : directSubscribers;
        this.sharedSubscribersByGroup = sharedSubscribersByGroup == null
            ? Collections.emptyMap()
            : sharedSubscribersByGroup;
    }

    public Set<String> getDirectSubscribers() {
        return directSubscribers;
    }

    public Map<String, Set<String>> getSharedSubscribersByGroup() {
        return sharedSubscribersByGroup;
    }
}
