package com.jmqx.router.global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Topic filter trie with MQTT wildcard support (+, #).
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class TopicTrie<V> {
    private final Node<V> root = new Node<>();

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

    private V computeIfAbsent(Node<V> node, String[] levels, int index, Supplier<V> supplier) {
        if (index >= levels.length) {
            if (node.terminal == null) {
                node.terminal = supplier.get();
            }
            return node.terminal;
        }

        String level = levels[index];
        if ("#".equals(level) && index == levels.length - 1) {
            if (node.hash == null) {
                node.hash = supplier.get();
            }
            return node.hash;
        }

        Node<V> next = "+".equals(level)
            ? node.getOrCreateWildcard()
            : node.children.computeIfAbsent(level, ignored -> new Node<>());
        return computeIfAbsent(next, levels, index + 1, supplier);
    }

    private V get(Node<V> node, String[] levels, int index) {
        if (node == null) {
            return null;
        }
        if (index >= levels.length) {
            return node.terminal;
        }
        String level = levels[index];
        if ("#".equals(level) && index == levels.length - 1) {
            return node.hash;
        }
        Node<V> next = "+".equals(level) ? node.wildcard : node.children.get(level);
        return get(next, levels, index + 1);
    }

    private boolean remove(Node<V> node, String[] levels, int index) {
        if (node == null) {
            return false;
        }
        if (index >= levels.length) {
            node.terminal = null;
            return node.isEmpty();
        }

        String level = levels[index];
        if ("#".equals(level) && index == levels.length - 1) {
            node.hash = null;
            return node.isEmpty();
        }

        Node<V> next = "+".equals(level) ? node.wildcard : node.children.get(level);
        if (next == null) {
            return false;
        }
        boolean empty = remove(next, levels, index + 1);
        if (empty) {
            if ("+".equals(level)) {
                node.wildcard = null;
            } else {
                node.children.remove(level, next);
            }
        }
        return node.isEmpty();
    }

    private void collect(Node<V> node, String[] levels, int index, List<V> result) {
        collect(node, levels, index, result::add);
    }

    private void collect(Node<V> node, String[] levels, int index, Consumer<V> consumer) {
        if (node == null) {
            return;
        }
        if (node.hash != null) {
            consumer.accept(node.hash);
        }
        if (index >= levels.length) {
            if (node.terminal != null) {
                consumer.accept(node.terminal);
            }
            return;
        }
        String current = levels[index];
        collect(node.children.get(current), levels, index + 1, consumer);
        collect(node.wildcard, levels, index + 1, consumer);
    }

    private static String[] split(String topicOrFilter) {
        if (topicOrFilter == null || topicOrFilter.isBlank()) {
            return new String[]{""};
        }
        return topicOrFilter.split("/", -1);
    }

    private static final class Node<V> {
        private final Map<String, Node<V>> children = new HashMap<>();
        private Node<V> wildcard;
        private V terminal;
        private V hash;

        private Node<V> getOrCreateWildcard() {
            if (wildcard == null) {
                wildcard = new Node<>();
            }
            return wildcard;
        }

        private boolean isEmpty() {
            return children.isEmpty() && wildcard == null && terminal == null && hash == null;
        }
    }
}
