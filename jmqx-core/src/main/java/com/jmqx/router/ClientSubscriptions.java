package com.jmqx.router;

import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-client subscriptions with cached immutable snapshot to reduce copy overhead.
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class ClientSubscriptions {
    private final ConcurrentMap<String, Byte> values = new ConcurrentHashMap<>();
    private final AtomicReference<Map<String, Integer>> snapshotRef = new AtomicReference<>();

    /**
     * Put or update subscription QoS.
     *
     * @return true when this is a new subscription key for the client.
     */
    public boolean put(String topicFilter, int qos) {
        Byte previous = values.put(topicFilter, normalizeQos(qos));
        snapshotRef.set(null);
        return previous == null;
    }

    public boolean removeIfPresent(String topicFilter) {
        Byte removed = values.remove(topicFilter);
        if (removed == null) {
            return false;
        }
        snapshotRef.set(null);
        return true;
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public Set<String> keySetSnapshot() {
        return Collections.unmodifiableSet(new HashSet<>(values.keySet()));
    }

    public Map<String, Integer> snapshot() {
        Map<String, Integer> cached = snapshotRef.get();
        if (cached != null) {
            return cached;
        }
        HashMap<String, Integer> mutableSnapshot = new HashMap<>(values.size());
        values.forEach((topic, qos) -> mutableSnapshot.put(topic, qos.intValue()));
        Map<String, Integer> fresh = Collections.unmodifiableMap(mutableSnapshot);
        if (snapshotRef.compareAndSet(null, fresh)) {
            return fresh;
        }
        return snapshotRef.get();
    }

    private static byte normalizeQos(int qos) {
        if (qos <= 0) {
            return 0;
        }
        if (qos >= 2) {
            return 2;
        }
        return 1;
    }
}
