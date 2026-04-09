package com.jmqx.router;

import com.jmqx.common.SharedSubscription;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * Local in-memory subscription registry for a single node.
 * This implementation uses a topic trie and read/write lock to stabilize publish matching under concurrent updates.
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class LocalSubscriptionRegistry implements SubscriptionRegistry {
    /**
     * Client-level subscription snapshot: clientId -> (topicFilter -> qos).
     */
    private final ConcurrentMap<String, ClientSubscriptions> subscriptionsByClient = new ConcurrentHashMap<>();

    /**
     * Local topic trie for routing match.
     */
    private final TopicTrieNode localTopicTrieNode = new TopicTrieNode();
    /**
     * topicFilter -> local subscription ref count on this node.
     */
    private final ConcurrentMap<String, AtomicInteger> topicRefCount = new ConcurrentHashMap<>();
    /**
     * Canonical topic filter pool to reduce duplicated String objects.
     */
    private final TopicFilterPool topicFilterPool = new TopicFilterPool();

    /**
     * Guards trie structural mutation and read traversal.
     */
    private final StampedLock trieLock = new StampedLock();

    @Override
    public boolean subscribeAndCheckFirst(String clientId, String topicFilter, int qos) {
        if (clientId == null || clientId.isBlank() || topicFilter == null || topicFilter.isBlank()) {
            return false;
        }
        String canonicalTopicFilter = topicFilterPool.acquire(topicFilter);
        boolean added = subscriptionsByClient
            .computeIfAbsent(clientId, ignored -> new ClientSubscriptions())
            .put(canonicalTopicFilter, qos);
        boolean firstLocal = false;
        if (added) {
            int refs = topicRefCount
                .computeIfAbsent(canonicalTopicFilter, ignored -> new AtomicInteger(0))
                .incrementAndGet();
            firstLocal = refs == 1;
        } else {
            topicFilterPool.release(canonicalTopicFilter);
        }
        long stamp = trieLock.writeLock();
        try {
            addToTrie(clientId, canonicalTopicFilter);
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
        String canonicalTopicFilter = topicFilterPool.resolve(topicFilter);
        ClientSubscriptions clientSubscriptions = subscriptionsByClient.get(clientId);
        if (clientSubscriptions == null) {
            return false;
        }
        if (!clientSubscriptions.removeIfPresent(canonicalTopicFilter)) {
            return false;
        }
        boolean lastLocal = false;
        AtomicInteger ref = topicRefCount.get(canonicalTopicFilter);
        if (ref != null && ref.decrementAndGet() <= 0) {
            topicRefCount.remove(canonicalTopicFilter, ref);
            lastLocal = true;
        }
        long stamp = trieLock.writeLock();
        try {
            removeFromTrie(clientId, canonicalTopicFilter);
        } finally {
            trieLock.unlockWrite(stamp);
        }
        topicFilterPool.release(canonicalTopicFilter);
        if (clientSubscriptions.isEmpty()) {
            subscriptionsByClient.remove(clientId, clientSubscriptions);
        }
        return lastLocal;
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
        Set<String> topicFilters = subscriptions.keySetSnapshot();
        Set<String> lastTopics = new HashSet<>();
        topicFilters.forEach(topicFilter -> {
            AtomicInteger ref = topicRefCount.get(topicFilter);
            if (ref != null && ref.decrementAndGet() <= 0) {
                topicRefCount.remove(topicFilter, ref);
                lastTopics.add(topicFilter);
            }
            topicFilterPool.release(topicFilter);
        });
        long stamp = trieLock.writeLock();
        try {
            topicFilters.forEach(topicFilter -> removeFromTrie(clientId, topicFilter));
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

}
