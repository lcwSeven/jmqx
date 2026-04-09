package com.jmqx.router;

import com.jmqx.common.SharedSubscription;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
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
     * Topic trie root.
     */
    private final TopicTrieNode root = new TopicTrieNode();

    /**
     * Guards trie structural mutation and read traversal.
     */
    private final StampedLock trieLock = new StampedLock();

    @Override
    public void subscribe(String clientId, String topicFilter, int qos) {
        if (clientId == null || clientId.isBlank() || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        subscriptionsByClient
            .computeIfAbsent(clientId, ignored -> new ClientSubscriptions())
            .put(topicFilter, qos);
        long stamp = trieLock.writeLock();
        try {
            addToTrie(clientId, topicFilter);
        } finally {
            trieLock.unlockWrite(stamp);
        }
    }

    @Override
    public void unsubscribe(String clientId, String topicFilter) {
        if (clientId == null || clientId.isBlank() || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        ClientSubscriptions clientSubscriptions = subscriptionsByClient.get(clientId);
        if (clientSubscriptions == null) {
            return;
        }
        if (!clientSubscriptions.removeIfPresent(topicFilter)) {
            return;
        }
        long stamp = trieLock.writeLock();
        try {
            removeFromTrie(clientId, topicFilter);
        } finally {
            trieLock.unlockWrite(stamp);
        }
        if (clientSubscriptions.isEmpty()) {
            subscriptionsByClient.remove(clientId, clientSubscriptions);
        }
    }

    @Override
    public void removeClient(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        ClientSubscriptions subscriptions = subscriptionsByClient.remove(clientId);
        if (subscriptions == null || subscriptions.isEmpty()) {
            return;
        }
        Set<String> topicFilters = subscriptions.keySetSnapshot();
        long stamp = trieLock.writeLock();
        try {
            topicFilters.forEach(topicFilter -> removeFromTrie(clientId, topicFilter));
        } finally {
            trieLock.unlockWrite(stamp);
        }
    }

    @Override
    public Set<String> findSubscribers(String topic) {
        SubscriptionMatchResult matchResult = findSubscriptionMatch(topic);
        Set<String> result = new LinkedHashSet<>(matchResult.getDirectSubscribers());
        matchResult.getSharedSubscribersByGroup().values().forEach(result::addAll);
        return result;
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
        collectMatches(root, topicLevels, 0, directSubscribers, sharedSubscribersByGroup);
        return new SubscriptionMatchResult(directSubscribers, sharedSubscribersByGroup);
    }

    private void addToTrie(String clientId, String topicFilter) {
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        if (shared != null) {
            insertFilter(root, splitLevels(shared.topicFilter()), 0, clientId, shared.group(), true);
            return;
        }
        insertFilter(root, splitLevels(topicFilter), 0, clientId, null, false);
    }

    private void removeFromTrie(String clientId, String topicFilter) {
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        if (shared != null) {
            removeFilter(root, splitLevels(shared.topicFilter()), 0, clientId, shared.group(), true);
            return;
        }
        removeFilter(root, splitLevels(topicFilter), 0, clientId, null, false);
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
            : node.children.computeIfAbsent(level, ignored -> new TopicTrieNode());
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

        TopicTrieNode nextNode = "+".equals(level) ? node.wildcardChild : node.children.get(level);
        if (nextNode == null) {
            return false;
        }
        boolean childEmpty = removeFilter(nextNode, levels, levelIndex + 1, clientId, sharedGroup, shared);
        if (childEmpty) {
            if ("+".equals(level)) {
                node.clearWildcardChild(nextNode);
            } else {
                node.children.remove(level, nextNode);
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
        collectMatches(node.children.get(currentLevel), topicLevels, levelIndex + 1, directSubscribers, sharedSubscribersByGroup);
        collectMatches(node.wildcardChild, topicLevels, levelIndex + 1, directSubscribers, sharedSubscribersByGroup);
    }

    private static String[] splitLevels(String topic) {
        return topic.split("/", -1);
    }

    /**
     * Trie node that stores direct and shared subscribers for terminal and hash matches.
     */
    private static class TopicTrieNode {
        private final ConcurrentMap<String, TopicTrieNode> children = new ConcurrentHashMap<>();
        private volatile TopicTrieNode wildcardChild;
        private final Set<String> terminalSubscribers = ConcurrentHashMap.newKeySet();
        private final Set<String> hashSubscribers = ConcurrentHashMap.newKeySet();
        private final ConcurrentMap<String, Set<String>> terminalSharedSubscribers = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Set<String>> hashSharedSubscribers = new ConcurrentHashMap<>();

        private TopicTrieNode getOrCreateWildcardChild() {
            TopicTrieNode current = wildcardChild;
            if (current != null) {
                return current;
            }
            synchronized (this) {
                if (wildcardChild == null) {
                    wildcardChild = new TopicTrieNode();
                }
                return wildcardChild;
            }
        }

        private void clearWildcardChild(TopicTrieNode expected) {
            if (wildcardChild == expected) {
                wildcardChild = null;
            }
        }

        private void addTerminal(String clientId, String group, boolean shared) {
            if (!shared) {
                terminalSubscribers.add(clientId);
                return;
            }
            terminalSharedSubscribers.computeIfAbsent(group, ignored -> ConcurrentHashMap.newKeySet()).add(clientId);
        }

        private void removeTerminal(String clientId, String group, boolean shared) {
            if (!shared) {
                terminalSubscribers.remove(clientId);
                return;
            }
            removeSharedClient(terminalSharedSubscribers, group, clientId);
        }

        private void addHash(String clientId, String group, boolean shared) {
            if (!shared) {
                hashSubscribers.add(clientId);
                return;
            }
            hashSharedSubscribers.computeIfAbsent(group, ignored -> ConcurrentHashMap.newKeySet()).add(clientId);
        }

        private void removeHash(String clientId, String group, boolean shared) {
            if (!shared) {
                hashSubscribers.remove(clientId);
                return;
            }
            removeSharedClient(hashSharedSubscribers, group, clientId);
        }

        private void collectTerminal(Set<String> directSubscribers, Map<String, Set<String>> sharedSubscribersByGroup) {
            directSubscribers.addAll(terminalSubscribers);
            collectShared(terminalSharedSubscribers, sharedSubscribersByGroup);
        }

        private void collectHash(Set<String> directSubscribers, Map<String, Set<String>> sharedSubscribersByGroup) {
            directSubscribers.addAll(hashSubscribers);
            collectShared(hashSharedSubscribers, sharedSubscribersByGroup);
        }

        private void collectShared(Map<String, Set<String>> source, Map<String, Set<String>> target) {
            source.forEach((group, subscribers) ->
                target.computeIfAbsent(group, ignored -> new HashSet<>()).addAll(subscribers)
            );
        }

        private boolean isEmpty() {
            return children.isEmpty()
                && wildcardChild == null
                && terminalSubscribers.isEmpty()
                && hashSubscribers.isEmpty()
                && terminalSharedSubscribers.isEmpty()
                && hashSharedSubscribers.isEmpty();
        }

        private static void removeSharedClient(
            ConcurrentMap<String, Set<String>> sharedSubscribers,
            String group,
            String clientId
        ) {
            if (group == null) {
                return;
            }
            Set<String> clients = sharedSubscribers.get(group);
            if (clients == null) {
                return;
            }
            clients.remove(clientId);
            if (clients.isEmpty()) {
                sharedSubscribers.remove(group, clients);
            }
        }
    }

    /**
     * Per-client subscriptions with cached immutable snapshot to reduce copy overhead.
     */
    private static class ClientSubscriptions {
        private final ConcurrentMap<String, Integer> values = new ConcurrentHashMap<>();
        private final AtomicReference<Map<String, Integer>> snapshotRef = new AtomicReference<>();

        private void put(String topicFilter, int qos) {
            values.put(topicFilter, qos);
            snapshotRef.set(null);
        }

        private boolean removeIfPresent(String topicFilter) {
            Integer removed = values.remove(topicFilter);
            if (removed == null) {
                return false;
            }
            snapshotRef.set(null);
            return true;
        }

        private boolean isEmpty() {
            return values.isEmpty();
        }

        private Set<String> keySetSnapshot() {
            return Set.copyOf(values.keySet());
        }

        private Map<String, Integer> snapshot() {
            Map<String, Integer> cached = snapshotRef.get();
            if (cached != null) {
                return cached;
            }
            Map<String, Integer> fresh = Map.copyOf(values);
            if (snapshotRef.compareAndSet(null, fresh)) {
                return fresh;
            }
            return snapshotRef.get();
        }
    }
}
