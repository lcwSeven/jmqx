package com.jmqx.router;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Trie node that stores direct and shared subscribers for terminal and hash matches.
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class TopicTrieNode {
    private final ConcurrentMap<String, TopicTrieNode> children = new ConcurrentHashMap<>();
    private volatile TopicTrieNode wildcardChild;
    private final Set<String> terminalSubscribers = ConcurrentHashMap.newKeySet();
    private final Set<String> hashSubscribers = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, Set<String>> terminalSharedSubscribers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> hashSharedSubscribers = new ConcurrentHashMap<>();

    public TopicTrieNode getOrCreateWildcardChild() {
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

    public TopicTrieNode getOrCreateLiteralChild(String level) {
        return children.computeIfAbsent(level, ignored -> new TopicTrieNode());
    }

    public TopicTrieNode getLiteralChild(String level) {
        return children.get(level);
    }

    public void removeLiteralChild(String level, TopicTrieNode expected) {
        children.remove(level, expected);
    }

    public TopicTrieNode getWildcardChild() {
        return wildcardChild;
    }

    public void clearWildcardChild(TopicTrieNode expected) {
        if (wildcardChild == expected) {
            wildcardChild = null;
        }
    }

    public void addTerminal(String clientId, String group, boolean shared) {
        if (!shared) {
            terminalSubscribers.add(clientId);
            return;
        }
        terminalSharedSubscribers.computeIfAbsent(group, ignored -> ConcurrentHashMap.newKeySet()).add(clientId);
    }

    public void removeTerminal(String clientId, String group, boolean shared) {
        if (!shared) {
            terminalSubscribers.remove(clientId);
            return;
        }
        removeSharedClient(terminalSharedSubscribers, group, clientId);
    }

    public void addHash(String clientId, String group, boolean shared) {
        if (!shared) {
            hashSubscribers.add(clientId);
            return;
        }
        hashSharedSubscribers.computeIfAbsent(group, ignored -> ConcurrentHashMap.newKeySet()).add(clientId);
    }

    public void removeHash(String clientId, String group, boolean shared) {
        if (!shared) {
            hashSubscribers.remove(clientId);
            return;
        }
        removeSharedClient(hashSharedSubscribers, group, clientId);
    }

    public void collectTerminal(Set<String> directSubscribers, Map<String, Set<String>> sharedSubscribersByGroup) {
        directSubscribers.addAll(terminalSubscribers);
        collectShared(terminalSharedSubscribers, sharedSubscribersByGroup);
    }

    public void collectHash(Set<String> directSubscribers, Map<String, Set<String>> sharedSubscribersByGroup) {
        directSubscribers.addAll(hashSubscribers);
        collectShared(hashSharedSubscribers, sharedSubscribersByGroup);
    }

    public boolean isEmpty() {
        return children.isEmpty()
            && wildcardChild == null
            && terminalSubscribers.isEmpty()
            && hashSubscribers.isEmpty()
            && terminalSharedSubscribers.isEmpty()
            && hashSharedSubscribers.isEmpty();
    }

    private void collectShared(Map<String, Set<String>> source, Map<String, Set<String>> target) {
        source.forEach((group, subscribers) ->
            target.computeIfAbsent(group, ignored -> new HashSet<>()).addAll(subscribers)
        );
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
