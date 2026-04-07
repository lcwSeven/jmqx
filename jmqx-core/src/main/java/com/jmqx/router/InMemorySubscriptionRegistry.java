package com.jmqx.router;

import com.jmqx.common.SharedSubscription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 基于内存的订阅注册表。
 * 当前实现使用 Topic Trie 来替代全量扫描，降低 publish 路径上的订阅匹配成本。
 *
 * 数据结构说明：
 * 1. subscriptionsByClient：客户端 -> 订阅过滤器/QoS，主要用于管理后台查询和 removeClient 快速清理。
 * 2. root(TopicTrieNode)：按 topic filter 分层索引，主要用于 publish 时快速匹配订阅者。
 * 3. sharedGroupIndexes：共享订阅组轮询游标，保证同组内近似均衡分发。
 *
 * @author liucaiwen
 * @date 2026/4/2
 */
public class InMemorySubscriptionRegistry implements SubscriptionRegistry {
    /**
     * 客户端维度的订阅快照：clientId -> (topicFilter -> qos)。
     */
    private final ConcurrentMap<String, ClientSubscriptions> subscriptionsByClient = new ConcurrentHashMap<>();

    /**
     * 共享订阅组轮询索引：group -> nextIndex。
     */
    private final ConcurrentMap<String, AtomicInteger> sharedGroupIndexes = new ConcurrentHashMap<>();

    /**
     * Topic Trie 根节点。
     */
    private final TopicTrieNode root = new TopicTrieNode();

    /**
     * 注册订阅。
     * 同时写入客户端视图与 Trie 视图，保证管理查询和消息路由都能快速执行。
     */
    @Override
    public void subscribe(String clientId, String topicFilter, int qos) {
        subscriptionsByClient
            .computeIfAbsent(clientId, ignored -> new ClientSubscriptions())
            .put(topicFilter, qos);
        // 持久保存一份 client -> subscriptions 方便后台展示，同时把过滤器写入 Trie 供快速匹配。
        addToTrie(clientId, topicFilter);
    }

    /**
     * 取消订阅。
     * 会从客户端视图和 Trie 视图同时删除。
     */
    @Override
    public void unsubscribe(String clientId, String topicFilter) {
        ClientSubscriptions clientSubscriptions = subscriptionsByClient.get(clientId);
        if (clientSubscriptions == null) {
            return;
        }
        clientSubscriptions.remove(topicFilter);
        removeFromTrie(clientId, topicFilter);
    }

    /**
     * 移除客户端及其全部订阅。
     */
    @Override
    public void removeClient(String clientId) {
        ClientSubscriptions subscriptions = subscriptionsByClient.remove(clientId);
        if (subscriptions == null || subscriptions.isEmpty()) {
            return;
        }
        subscriptions.keySet().forEach(topicFilter -> removeFromTrie(clientId, topicFilter));
    }

    /**
     * 根据发布 topic 匹配订阅者。
     * 普通订阅全部命中；共享订阅按 group 只挑选一个客户端返回。
     */
    @Override
    public Set<String> findSubscribers(String topic) {
        Set<String> directSubscribers = new LinkedHashSet<>();
        Map<String, Set<String>> sharedSubscribersByGroup = new HashMap<>();
        String[] topicLevels = splitLevels(topic);
        collectMatches(root, topicLevels, 0, directSubscribers, sharedSubscribersByGroup);

        // 共享订阅同组内只选择一个客户端，普通订阅则全部下发。
        Set<String> result = new LinkedHashSet<>(directSubscribers);
        sharedSubscribersByGroup.forEach((group, candidates) -> {
            String selected = pickSharedSubscriber(group, candidates);
            if (selected != null) {
                result.add(selected);
            }
        });
        return result;
    }

    /**
     * 查询指定客户端当前全部订阅（只读快照）。
     */
    @Override
    public Map<String, Integer> findSubscriptions(String clientId) {
        ClientSubscriptions subscriptions = subscriptionsByClient.get(clientId);
        if (subscriptions == null) {
            return Collections.emptyMap();
        }
        return subscriptions.snapshot();
    }

    /**
     * 把订阅过滤器写入 Trie。
     * 共享订阅会先规范化为真实 topicFilter + group，再写入索引。
     */
    private void addToTrie(String clientId, String topicFilter) {
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        if (shared != null) {
            insertFilter(root, splitLevels(shared.topicFilter()), 0, clientId, shared.group(), true);
            return;
        }
        insertFilter(root, splitLevels(topicFilter), 0, clientId, null, false);
    }

    /**
     * 从 Trie 删除订阅过滤器。
     */
    private void removeFromTrie(String clientId, String topicFilter) {
        SharedSubscription.Parsed shared = SharedSubscription.parse(topicFilter);
        if (shared != null) {
            removeFilter(root, splitLevels(shared.topicFilter()), 0, clientId, shared.group(), true);
            return;
        }
        removeFilter(root, splitLevels(topicFilter), 0, clientId, null, false);
    }

    /**
     * 递归插入过滤器到 Trie。
     */
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
            // 多层通配符只能出现在末尾，直接挂到当前节点的 hash 桶上。
            node.addHash(clientId, sharedGroup, shared);
            return;
        }

        TopicTrieNode nextNode = "+".equals(level)
            ? node.getOrCreateWildcardChild()
            : node.children.computeIfAbsent(level, ignored -> new TopicTrieNode());
        insertFilter(nextNode, levels, levelIndex + 1, clientId, sharedGroup, shared);
    }

    /**
     * 递归删除过滤器，并返回当前节点是否已为空（供父节点做子树回收）。
     */
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

    /**
     * 递归收集匹配结果。
     *
     * 匹配顺序：
     * 1. 先收集当前层 '#' 订阅（可匹配当前及后续层）。
     * 2. 若 topic 已遍历完，收集当前层精确结束订阅。
     * 3. 否则继续沿字面量子节点和 '+' 子节点下探。
     */
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

        // '#' 能匹配从当前层开始的所有后续层，因此每次下探前都要先收集。
        node.collectHash(directSubscribers, sharedSubscribersByGroup);
        if (levelIndex >= topicLevels.length) {
            node.collectTerminal(directSubscribers, sharedSubscribersByGroup);
            return;
        }

        String currentLevel = topicLevels[levelIndex];
        // 先走字面量分支，再走 '+' 单层通配符分支。
        collectMatches(node.children.get(currentLevel), topicLevels, levelIndex + 1, directSubscribers, sharedSubscribersByGroup);
        collectMatches(node.wildcardChild, topicLevels, levelIndex + 1, directSubscribers, sharedSubscribersByGroup);
    }

    /**
     * 按 MQTT 规则保留空 level 切分（split("/", -1)）。
     */
    private static String[] splitLevels(String topic) {
        return topic.split("/", -1);
    }

    /**
     * 共享订阅组内选择一个客户端。
     * 当前实现按组内候选做轮询，不保证强一致，仅保证单节点内近似均衡。
     */
    private String pickSharedSubscriber(String group, Set<String> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        List<String> list = new ArrayList<>(candidates);
        AtomicInteger idx = sharedGroupIndexes.computeIfAbsent(group, ignored -> new AtomicInteger(0));
        int current = Math.floorMod(idx.getAndIncrement(), list.size());
        return list.get(current);
    }

    /**
     * Trie 节点同时维护 4 类数据：
     * 1. 普通精确结束订阅
     * 2. 普通 '#' 订阅
     * 3. 共享精确结束订阅
     * 4. 共享 '#' 订阅
     *
     * @author liucaiwen
     * @date 2026/4/7
     */
    private static class TopicTrieNode {
        /**
         * 字面量子节点（例如 "a"、"sensor"）。
         */
        private final ConcurrentMap<String, TopicTrieNode> children = new ConcurrentHashMap<>();

        /**
         * 单层通配符 '+' 子节点（每层最多一个）。
         */
        private volatile TopicTrieNode wildcardChild;

        /**
         * 当前层“精确结束”的普通订阅者。
         */
        private final Set<String> terminalSubscribers = ConcurrentHashMap.newKeySet();

        /**
         * 当前层 '#' 普通订阅者。
         */
        private final Set<String> hashSubscribers = ConcurrentHashMap.newKeySet();

        /**
         * 当前层“精确结束”的共享订阅：group -> clients。
         */
        private final ConcurrentMap<String, Set<String>> terminalSharedSubscribers = new ConcurrentHashMap<>();

        /**
         * 当前层 '#' 共享订阅：group -> clients。
         */
        private final ConcurrentMap<String, Set<String>> hashSharedSubscribers = new ConcurrentHashMap<>();

        /**
         * 懒加载创建 '+' 子节点。
         */
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

        /**
         * 仅在当前引用未变化时清除 '+' 子节点，避免并发误删。
         */
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

        private void collectShared(
            Map<String, Set<String>> source,
            Map<String, Set<String>> target
        ) {
            source.forEach((group, subscribers) ->
                target.computeIfAbsent(group, ignored -> new HashSet<>()).addAll(subscribers)
            );
        }

        /**
         * 判断当前节点是否可回收。
         */
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
     * 每个客户端的订阅集合。
     *
     * 设计目标：
     * 1. 写操作（订阅变更）直接改并发 map，并使缓存快照失效。
     * 2. 读操作（后台查询）优先返回缓存快照，避免高频 Map.copyOf 带来的分配开销。
     */
    private static class ClientSubscriptions {
        private final ConcurrentMap<String, Integer> values = new ConcurrentHashMap<>();
        private final AtomicReference<Map<String, Integer>> snapshotRef = new AtomicReference<>();

        private void put(String topicFilter, int qos) {
            values.put(topicFilter, qos);
            snapshotRef.set(null);
        }

        private void remove(String topicFilter) {
            values.remove(topicFilter);
            snapshotRef.set(null);
        }

        private boolean isEmpty() {
            return values.isEmpty();
        }

        private Set<String> keySet() {
            return values.keySet();
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
