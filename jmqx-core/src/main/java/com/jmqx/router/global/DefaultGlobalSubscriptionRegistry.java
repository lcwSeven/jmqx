package com.jmqx.router.global;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory global subscription registry.
 * <p>
 * Design:
 * 1. Normal subscriptions are indexed by topic trie: topicFilter -> node set.
 * 2. Shared subscriptions are indexed by (group, topicFilter): group -> trie(topicFilter -> node set).
 * 3. Reverse index supports node-down cleanup: node -> topic keys.
 * 4. apply(logIndex) is idempotent by appliedLogIndex gate.
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class DefaultGlobalSubscriptionRegistry implements GlobalSubscriptionRegistry {
    private static final String NORMAL_PREFIX = "n|";
    private static final String SHARED_PREFIX = "s|";

    private final TopicTrie<Set<String>> normalTopicToNodes = new TopicTrie<>();
    private final ConcurrentMap<String, TopicTrie<Set<String>>> sharedGroupToTopicToNodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> nodeToTopicKeys = new ConcurrentHashMap<>();
    private final AtomicLong appliedLogIndex = new AtomicLong(0);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public void apply(GlobalSubscriptionEvent event) {
        if (event == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            if (event.getLogIndex() <= appliedLogIndex.get()) {
                return;
            }
            applyEventInternal(event);
            appliedLogIndex.set(event.getLogIndex());
        } finally {
            lock.writeLock().unlock();
        }
    }


    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            normalTopicToNodes.clear();
            sharedGroupToTopicToNodes.clear();
            nodeToTopicKeys.clear();
            appliedLogIndex.set(0L);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public GlobalSubscriptionMatch match(String topic) {
        lock.readLock().lock();
        try {
            Set<String> normalNodes = new LinkedHashSet<>();
            normalTopicToNodes.forEachMatch(topic, normalNodes::addAll);

            Map<String, Set<String>> shared = new HashMap<>();
            sharedGroupToTopicToNodes.forEach((group, trie) -> {
                Set<String> nodes = new LinkedHashSet<>();
                trie.forEachMatch(topic, nodes::addAll);
                if (!nodes.isEmpty()) {
                    shared.put(group, nodes);
                }
            });
            return new GlobalSubscriptionMatch(normalNodes, shared);
        } finally {
            lock.readLock().unlock();
        }
    }


    @Override
    public Map<String, Set<String>> snapshotNodeToTopicKeys() {
        lock.readLock().lock();
        try {
            Map<String, Set<String>> snapshot = new HashMap<>();
            nodeToTopicKeys.forEach((node, keys) -> snapshot.put(node, Set.copyOf(keys)));
            return snapshot;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long appliedLogIndex() {
        return appliedLogIndex.get();
    }

    private void applyEventInternal(GlobalSubscriptionEvent event) {
        if (event.getType() == GlobalSubscriptionEventType.REGISTER) {
            applyRegister(event.getNodeId(), event.getTopicFilter(), event.getSharedGroup());
        } else {
            applyUnregister(event.getNodeId(), event.getTopicFilter(), event.getSharedGroup());
        }
    }

    private void applyRegister(String nodeId, String topicFilter, String sharedGroup) {
        if (nodeId == null || nodeId.isBlank() || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        TopicKey topicKey = TopicKey.of(topicFilter, sharedGroup);
        Set<String> topicKeys = nodeToTopicKeys.computeIfAbsent(nodeId, ignored -> ConcurrentHashMap.newKeySet());
        if (!topicKeys.add(topicKey.raw)) {
            return;
        }

        if (topicKey.isShared()) {
            TopicTrie<Set<String>> trie = sharedGroupToTopicToNodes.computeIfAbsent(
                    topicKey.sharedGroup,
                    ignored -> new TopicTrie<>()
            );
            trie.computeIfAbsent(topicKey.topicFilter, ConcurrentHashMap::newKeySet).add(nodeId);
            return;
        }
        normalTopicToNodes.computeIfAbsent(topicKey.topicFilter, ConcurrentHashMap::newKeySet).add(nodeId);
    }

    private void applyUnregister(String nodeId, String topicFilter, String sharedGroup) {
        if (nodeId == null || nodeId.isBlank() || topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        TopicKey topicKey = TopicKey.of(topicFilter, sharedGroup);
        Set<String> topicKeys = nodeToTopicKeys.get(nodeId);
        if (topicKeys == null || !topicKeys.remove(topicKey.raw)) {
            return;
        }
        if (topicKeys.isEmpty()) {
            nodeToTopicKeys.remove(nodeId, topicKeys);
        }

        if (topicKey.isShared()) {
            TopicTrie<Set<String>> trie = sharedGroupToTopicToNodes.get(topicKey.sharedGroup);
            if (trie == null) {
                return;
            }
            Set<String> nodes = trie.get(topicKey.topicFilter);
            if (nodes != null) {
                nodes.remove(nodeId);
                if (nodes.isEmpty()) {
                    trie.remove(topicKey.topicFilter);
                }
            }
            if (trie.isEmpty()) {
                sharedGroupToTopicToNodes.remove(topicKey.sharedGroup, trie);
            }
            return;
        }

        Set<String> nodes = normalTopicToNodes.get(topicKey.topicFilter);
        if (nodes != null) {
            nodes.remove(nodeId);
            if (nodes.isEmpty()) {
                normalTopicToNodes.remove(topicKey.topicFilter);
            }
        }
    }

    private static final class TopicKey {
        private final String raw;
        private final String topicFilter;
        private final String sharedGroup;

        private TopicKey(String raw, String topicFilter, String sharedGroup) {
            this.raw = raw;
            this.topicFilter = topicFilter;
            this.sharedGroup = sharedGroup;
        }

        private static TopicKey of(String topicFilter, String sharedGroup) {
            if (sharedGroup == null || sharedGroup.isBlank()) {
                return new TopicKey(NORMAL_PREFIX + topicFilter, topicFilter, null);
            }
            return new TopicKey(SHARED_PREFIX + sharedGroup + "|" + topicFilter, topicFilter, sharedGroup);
        }

        private static TopicKey parse(String raw) {
            if (raw == null || raw.isBlank()) {
                return null;
            }
            if (raw.startsWith(NORMAL_PREFIX)) {
                String topicFilter = raw.substring(NORMAL_PREFIX.length());
                return new TopicKey(raw, topicFilter, null);
            }
            if (raw.startsWith(SHARED_PREFIX)) {
                String remaining = raw.substring(SHARED_PREFIX.length());
                int idx = remaining.indexOf('|');
                if (idx <= 0 || idx >= remaining.length() - 1) {
                    return null;
                }
                String group = remaining.substring(0, idx);
                String topicFilter = remaining.substring(idx + 1);
                return new TopicKey(raw, topicFilter, group);
            }
            return null;
        }

        private boolean isShared() {
            return sharedGroup != null && !sharedGroup.isBlank();
        }
    }
}
