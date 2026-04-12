package com.jmqx.router;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 客户端订阅快照容器。
 * 使用 topicFilter -> qos 的并发映射，优先保证并发正确性和实现清晰度。
 *
 * @author liucaiwen
 * @date 2026/4/11
 */
public class ClientSubscriptions {
    /**
     * 当前客户端的订阅集合，key 为 topicFilter，value 为 qos。
     */
    private final ConcurrentMap<String, Byte> subscriptions = new ConcurrentHashMap<>();

    /**
     * 新增或更新订阅。
     *
     * @return true 表示新增订阅；false 表示更新已有订阅
     */
    public boolean put(String topicFilter, int qos) {
        Byte normalized = normalizeQos(qos);
        Byte previous = subscriptions.put(topicFilter, normalized);
        return previous == null;
    }

    /**
     * 删除订阅。
     */
    public boolean removeIfPresent(String topicFilter) {
        return subscriptions.remove(topicFilter) != null;
    }

    /**
     * 当前客户端是否没有任何订阅。
     */
    public boolean isEmpty() {
        return subscriptions.isEmpty();
    }

    /**
     * 生成只读订阅快照（topicFilter -> qos）。
     */
    public Map<String, Integer> snapshot() {
        if (subscriptions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Integer> copy = new HashMap<>(subscriptions.size());
        subscriptions.forEach((topicFilter, qos) -> copy.put(topicFilter, (int) qos));
        return Collections.unmodifiableMap(copy);
    }

    private static byte normalizeQos(int qos) {
        if (qos <= 0) {
            return 0;
        }
        if (qos >= 2) {
            return 2;
        }
        return 1;
    }
}
