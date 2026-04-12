package com.jmqx.router.global;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Topic filter trie with MQTT wildcard support (+, #).
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class TopicTrie<V> {

    private final GenericTopicTrieNode<V> root = new GenericTopicTrieNode<>();

    public V computeIfAbsent(String topicFilter, Supplier<V> supplier) {
        String[] levels = split(topicFilter);
        return computeIfAbsent(root, levels, 0, supplier);
    }

    public V get(String topicFilter) {
        String[] levels = split(topicFilter);
        return get(root, levels, 0);
    }

    public void remove(String topicFilter) {
        String[] levels = split(topicFilter);
        remove(root, levels, 0);
    }

    public List<V> findMatches(String topic) {
        List<V> result = new ArrayList<>();
        String[] levels = split(topic);
        collect(root, levels, 0, result);
        return result;
    }

    /**
     * Streams matches without allocating an intermediate list.
     */
    public void forEachMatch(String topic, Consumer<V> consumer) {
        String[] levels = split(topic);
        collect(root, levels, 0, consumer);
    }

    public boolean isEmpty() {
        return root.isEmpty();
    }

    public void clear() {
        root.clear();
    }

    private V computeIfAbsent(GenericTopicTrieNode<V> node, String[] levels, int index, Supplier<V> supplier) {
        if (index >= levels.length) {
            if (node.getTerminalValue() == null) {
                node.setTerminalValue(supplier.get());
            }
            return node.getTerminalValue();
        }

        String level = levels[index];
        if ("#".equals(level) && index == levels.length - 1) {
            if (node.getHashValue() == null) {
                node.setHashValue(supplier.get());
            }
            return node.getHashValue();
        }

        GenericTopicTrieNode<V> next = "+".equals(level)
            ? node.getOrCreateWildcardChild()
            : node.getOrCreateLiteralChild(level);
        return computeIfAbsent(next, levels, index + 1, supplier);
    }

    private V get(GenericTopicTrieNode<V> node, String[] levels, int index) {
        if (node == null) {
            return null;
        }
        if (index >= levels.length) {
            return node.getTerminalValue();
        }
        String level = levels[index];
        if ("#".equals(level) && index == levels.length - 1) {
            return node.getHashValue();
        }
        GenericTopicTrieNode<V> next = "+".equals(level) ? node.getWildcardChild() : node.getLiteralChild(level);
        return get(next, levels, index + 1);
    }

    private boolean remove(GenericTopicTrieNode<V> node, String[] levels, int index) {
        if (node == null) {
            return false;
        }
        if (index >= levels.length) {
            node.setTerminalValue(null);
            return node.isEmpty();
        }

        String level = levels[index];
        if ("#".equals(level) && index == levels.length - 1) {
            node.setHashValue(null);
            return node.isEmpty();
        }

        GenericTopicTrieNode<V> next = "+".equals(level) ? node.getWildcardChild() : node.getLiteralChild(level);
        if (next == null) {
            return false;
        }
        boolean empty = remove(next, levels, index + 1);
        if (empty) {
            if ("+".equals(level)) {
                node.clearWildcardChild(next);
            } else {
                node.removeLiteralChild(level, next);
            }
        }
        return node.isEmpty();
    }

    private void collect(GenericTopicTrieNode<V> node, String[] levels, int index, List<V> result) {
        collect(node, levels, index, result::add);
    }

    private void collect(GenericTopicTrieNode<V> node, String[] levels, int index, Consumer<V> consumer) {
        if (node == null) {
            return;
        }
        if (node.getHashValue() != null) {
            consumer.accept(node.getHashValue());
        }
        if (index >= levels.length) {
            if (node.getTerminalValue() != null) {
                consumer.accept(node.getTerminalValue());
            }
            return;
        }
        String current = levels[index];
        collect(node.getLiteralChild(current), levels, index + 1, consumer);
        collect(node.getWildcardChild(), levels, index + 1, consumer);
    }

    private static String[] split(String topicOrFilter) {
        if (topicOrFilter == null || topicOrFilter.isBlank()) {
            return new String[]{""};
        }
        return topicOrFilter.split("/", -1);
    }
}
