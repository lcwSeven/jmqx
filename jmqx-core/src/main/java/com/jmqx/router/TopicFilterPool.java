package com.jmqx.router;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Topic filter canonicalization pool to reduce duplicated String instances in memory.
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class TopicFilterPool {
    private final ConcurrentMap<String, TopicRef> refs = new ConcurrentHashMap<>();

    /**
     * Acquire a canonical topic filter and increment reference count.
     */
    public String acquire(String topicFilter) {
        TopicRef ref = refs.compute(topicFilter, (key, existing) -> {
            if (existing == null) {
                return new TopicRef(key);
            }
            existing.increment();
            return existing;
        });
        return ref.topicFilter();
    }

    /**
     * Resolve to canonical string without changing reference count.
     */
    public String resolve(String topicFilter) {
        TopicRef ref = refs.get(topicFilter);
        return ref == null ? topicFilter : ref.topicFilter();
    }

    /**
     * Decrement ref count and evict when no longer used.
     */
    public void release(String topicFilter) {
        refs.computeIfPresent(topicFilter, (ignored, ref) -> ref.decrementAndGet() <= 0 ? null : ref);
    }

    private static final class TopicRef {
        private final String topicFilter;
        private final AtomicInteger refs = new AtomicInteger(1);

        private TopicRef(String topicFilter) {
            this.topicFilter = topicFilter;
        }

        private String topicFilter() {
            return topicFilter;
        }

        private void increment() {
            refs.incrementAndGet();
        }

        private int decrementAndGet() {
            return refs.decrementAndGet();
        }
    }
}
