package com.jmqx.router;

import com.jmqx.common.SharedSubscription;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * 单节点本地内存订阅注册表。
 * 基于 Topic Trie + 读写锁实现，在并发订阅变更场景下保持路由匹配稳定。
 * 说明：
 * 1. subscriptionsByClient 直接保存 topicFilter -> qos；
 * 2. topicRefCount 维护本地引用计数，保留 first/last 语义；
 * 3. Topic Trie 仅用于发布匹配路由。
 *
 * @author liucaiwen
 * @date 2026/4/11
 */
public class LocalSubscriptionRegistry implements SubscriptionRegistry {
    /**
     * 客户端维度订阅快照：clientId -> (topicFilter -> qos)。
     */
    private final ConcurrentMap<String, ClientSubscriptions> subscriptionsByClient = new ConcurrentHashMap<>();
    /**
     * 本地主题 Trie，用于发布路由匹配。
     */
    private final TopicTrieNode localTopicTrieNode = new TopicTrieNode();
    /**
     * 本地主题引用计数：topicFilter -> 引用次数。
     */
    private final ConcurrentMap<String, AtomicInteger> topicRefCount = new ConcurrentHashMap<>();
    /**
     * 保护 Trie 结构修改与读遍历一致性。
     */
    private final StampedLock trieLock = new StampedLock();

    @Override
    public boolean subscribeAndCheckFirst(String clientId, String topicFilter, int qos) {
        if (clientId == null || clientId.isBlank() || topicFilter == null || topicFilter.isBlank()) {
            return false;
        }
        boolean added = subscriptionsByClient
                .computeIfAbsent(clientId, ignored -> new ClientSubscriptions())
                .put(topicFilter, qos);
        if (!added) {
            return false;
        }
        boolean firstLocal = incrementAndCheckFirst(topicFilter);

        long stamp = trieLock.writeLock();
        try {
            addToTrie(clientId, topicFilter);
        } finally {
            trieLock.unlockWrite(stamp);
        }
        return firstLocal;
    }

    @Override
    public boolean unsubscribeAndCheckLast(String clientId, String topicFilter) {
        if (clientId == null || clientId.isBlank() || topicFilter == null || topicFilter.isBlank()) {
            return false;
        }
        ClientSubscriptions clientSubscriptions = subscriptionsByClient.get(clientId);
        if (clientSubscriptions == null) {
            return false;
        }
        if (!clientSubscriptions.removeIfPresent(topicFilter)) {
            return false;
        }
        boolean lastLocal = decrementAndCheckLast(topicFilter);
        long stamp = trieLock.writeLock();
        try {
            removeFromTrie(clientId, topicFilter);
        } finally {
            trieLock.unlockWrite(stamp);
        }
        if (clientSubscriptions.isEmpty()) {
            subscriptionsByClient.remove(clientId, clientSubscriptions);
        }
        return lastLocal;
    }

    @Override
    public Set<String> unsubscribeBatchAndCollectLast(String clientId, List<String> topicFilters) {
        if (clientId == null || clientId.isBlank() || topicFilters == null || topicFilters.isEmpty()) {
            return Collections.emptySet();
        }
        ClientSubscriptions clientSubscriptions = subscriptionsByClient.get(clientId);
        if (clientSubscriptions == null) {
            return Collections.emptySet();
        }
        Set<String> removedTopics = new LinkedHashSet<>();
        Set<String> lastTopics = new LinkedHashSet<>();
        for (String topicFilter : topicFilters) {
            if (topicFilter == null || topicFilter.isBlank()) {
                continue;
            }
            if (!clientSubscriptions.removeIfPresent(topicFilter)) {
                continue;
            }
            removedTopics.add(topicFilter);
            if (decrementAndCheckLast(topicFilter)) {
                lastTopics.add(topicFilter);
            }
        }
        if (removedTopics.isEmpty()) {
            return Collections.emptySet();
        }
        long stamp = trieLock.writeLock();
        try {
            removedTopics.forEach(topicFilter -> removeFromTrie(clientId, topicFilter));
        } finally {
            trieLock.unlockWrite(stamp);
        }
        if (clientSubscriptions.isEmpty()) {
            subscriptionsByClient.remove(clientId, clientSubscriptions);
        }
        return lastTopics;
    }

    @Override
    public Set<String> removeClientAndCollectLastTopics(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return Collections.emptySet();
        }
        ClientSubscriptions subscriptions = subscriptionsByClient.remove(clientId);
        if (subscriptions == null || subscriptions.isEmpty()) {
            return Collections.emptySet();
        }
        Map<String, Integer> snapshot = subscriptions.snapshot();
        Set<String> lastTopics = new HashSet<>();
        Set<String> allTopics = snapshot.keySet();
        for (String topicFilter : allTopics) {
            if (topicFilter == null || topicFilter.isBlank()) {
                continue;
            }
            if (decrementAndCheckLast(topicFilter)) {
                lastTopics.add(topicFilter);
            }
        }
        long stamp = trieLock.writeLock();
        try {
            allTopics.forEach(topicFilter -> removeFromTrie(clientId, topicFilter));
        } finally {
            trieLock.unlockWrite(stamp);
        }
        return lastTopics;
    }

    @Override
    public SubscriptionMatchResult findSubscriptionMatch(String topic) {
        if (topic == null || topic.isBlank()) {
            return new SubscriptionMatchResult(Collections.emptySet(), Collections.emptyMap());
        }
        String[] topicLevels = splitLevels(topic);
        long optimistic = trieLock.tryOptimisticRead();
        SubscriptionMatchResult optimisticResult = collectMatchResult(topicLevels);
        if (trieLock.validate(optimistic)) {
            return optimisticResult;
        }
        long stamp = trieLock.readLock();
        try {
            return collectMatchResult(topicLevels);
        } finally {
            trieLock.unlockRead(stamp);
        }
    }

    @Override
    public Map<String, Integer> findSubscriptions(String clientId) {
        ClientSubscriptions subscriptions = subscriptionsByClient.get(clientId);
        if (subscriptions == null) {
            return Collections.emptyMap();
        }
        return subscriptions.snapshot();
    }

    private SubscriptionMatchResult collectMatchResult(String[] topicLevels) {
        Set<String> directSubscribers = new LinkedHashSet<>();
        Map<String, Set<String>> sharedSubscribersByGroup = new HashMap<>();
        collectMatches(localTopicTrieNode, topicLevels, 0, directSubscribers, sharedSubscribersByGroup);
        return new SubscriptionMatchResult(directSubscribers, sharedSubscribersByGroup);
    }

    private void addToTrie(String clientId, String topicFilter) {
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        if (shared != null) {
            insertFilter(localTopicTrieNode, splitLevels(shared.topicFilter()), 0, clientId, shared.group(), true);
            return;
        }
        insertFilter(localTopicTrieNode, splitLevels(topicFilter), 0, clientId, null, false);
    }

    private void removeFromTrie(String clientId, String topicFilter) {
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        if (shared != null) {
            removeFilter(localTopicTrieNode, splitLevels(shared.topicFilter()), 0, clientId, shared.group(), true);
            return;
        }
        removeFilter(localTopicTrieNode, splitLevels(topicFilter), 0, clientId, null, false);
    }

    private void insertFilter(
            TopicTrieNode node,
            String[] levels,
            int levelIndex,
            String clientId,
            String sharedGroup,
            boolean shared
    ) {
        if (levelIndex >= levels.length) {
            node.addTerminal(clientId, sharedGroup, shared);
            return;
        }
        String level = levels[levelIndex];
        if ("#".equals(level) && levelIndex == levels.length - 1) {
            node.addHash(clientId, sharedGroup, shared);
            return;
        }
        TopicTrieNode nextNode = "+".equals(level)
                ? node.getOrCreateWildcardChild()
                : node.getOrCreateLiteralChild(level);
        insertFilter(nextNode, levels, levelIndex + 1, clientId, sharedGroup, shared);
    }

    private boolean removeFilter(
            TopicTrieNode node,
            String[] levels,
            int levelIndex,
            String clientId,
            String sharedGroup,
            boolean shared
    ) {
        if (levelIndex >= levels.length) {
            node.removeTerminal(clientId, sharedGroup, shared);
            return node.isEmpty();
        }
        String level = levels[levelIndex];
        if ("#".equals(level) && levelIndex == levels.length - 1) {
            node.removeHash(clientId, sharedGroup, shared);
            return node.isEmpty();
        }
        TopicTrieNode nextNode = "+".equals(level) ? node.getWildcardChild() : node.getLiteralChild(level);
        if (nextNode == null) {
            return false;
        }
        boolean childEmpty = removeFilter(nextNode, levels, levelIndex + 1, clientId, sharedGroup, shared);
        if (childEmpty) {
            if ("+".equals(level)) {
                node.clearWildcardChild(nextNode);
            } else {
                node.removeLiteralChild(level, nextNode);
            }
        }
        return node.isEmpty();
    }

    private void collectMatches(
            TopicTrieNode node,
            String[] topicLevels,
            int levelIndex,
            Set<String> directSubscribers,
            Map<String, Set<String>> sharedSubscribersByGroup
    ) {
        if (node == null) {
            return;
        }
        node.collectHash(directSubscribers, sharedSubscribersByGroup);
        if (levelIndex >= topicLevels.length) {
            node.collectTerminal(directSubscribers, sharedSubscribersByGroup);
            return;
        }
        String currentLevel = topicLevels[levelIndex];
        collectMatches(node.getLiteralChild(currentLevel), topicLevels, levelIndex + 1, directSubscribers, sharedSubscribersByGroup);
        collectMatches(node.getWildcardChild(), topicLevels, levelIndex + 1, directSubscribers, sharedSubscribersByGroup);
    }

    private static String[] splitLevels(String topic) {
        return topic.split("/", -1);
    }

    /**
     * 引用计数 +1，并返回是否为本节点首次订阅该主题。
     */
    private boolean incrementAndCheckFirst(String topicFilter) {
        AtomicInteger ref = topicRefCount.computeIfAbsent(topicFilter, ignored -> new AtomicInteger(0));
        return ref.incrementAndGet() == 1;
    }

    /**
     * 引用计数 减1，并返回是否已经成为 0（最后一个本地订阅已移除）。
     */
    private boolean decrementAndCheckLast(String topicFilter) {
        final boolean[] last = {false};
        topicRefCount.computeIfPresent(topicFilter, (ignored, ref) -> {
            int current = ref.decrementAndGet();
            if (current <= 0) {
                last[0] = true;
                return null;
            }
            return ref;
        });
        return last[0];
    }
}
