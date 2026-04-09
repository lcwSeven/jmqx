package com.jmqx.router.global;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic trie node for topic trie implementations.
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class GenericTopicTrieNode<V> {
    private final Map<String, GenericTopicTrieNode<V>> literalChildren = new HashMap<>();
    private GenericTopicTrieNode<V> wildcardChild;
    private V terminalValue;
    private V hashValue;

    public GenericTopicTrieNode<V> getOrCreateLiteralChild(String level) {
        return literalChildren.computeIfAbsent(level, ignored -> new GenericTopicTrieNode<>());
    }

    public GenericTopicTrieNode<V> getLiteralChild(String level) {
        return literalChildren.get(level);
    }

    public void removeLiteralChild(String level, GenericTopicTrieNode<V> expected) {
        literalChildren.remove(level, expected);
    }

    public GenericTopicTrieNode<V> getOrCreateWildcardChild() {
        if (wildcardChild == null) {
            wildcardChild = new GenericTopicTrieNode<>();
        }
        return wildcardChild;
    }

    public GenericTopicTrieNode<V> getWildcardChild() {
        return wildcardChild;
    }

    public void clearWildcardChild(GenericTopicTrieNode<V> expected) {
        if (wildcardChild == expected) {
            wildcardChild = null;
        }
    }

    public V getTerminalValue() {
        return terminalValue;
    }

    public void setTerminalValue(V terminalValue) {
        this.terminalValue = terminalValue;
    }

    public V getHashValue() {
        return hashValue;
    }

    public void setHashValue(V hashValue) {
        this.hashValue = hashValue;
    }

    public boolean isEmpty() {
        return literalChildren.isEmpty() && wildcardChild == null && terminalValue == null && hashValue == null;
    }

    public void clear() {
        literalChildren.clear();
        wildcardChild = null;
        terminalValue = null;
        hashValue = null;
    }
}
