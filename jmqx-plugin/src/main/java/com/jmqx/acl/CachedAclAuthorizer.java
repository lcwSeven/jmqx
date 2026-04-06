package com.jmqx.acl;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class CachedAclAuthorizer implements AclAuthorizer {
    private final AclAuthorizer delegate;
    private final long ttlMillis;
    private final ConcurrentMap<CacheKey, CacheValue> cache = new ConcurrentHashMap<>();
    private final AtomicInteger accessCounter = new AtomicInteger();

    public CachedAclAuthorizer(AclAuthorizer delegate, int ttlMillis) {
        this.delegate = delegate;
        this.ttlMillis = Math.max(ttlMillis, 0);
    }

    @Override
    public boolean isAllowed(AclRequest request) {
        if (ttlMillis <= 0) {
            return delegate.isAllowed(request);
        }
        long now = System.currentTimeMillis();
        CacheKey key = new CacheKey(request);
        CacheValue hit = cache.get(key);
        if (hit != null && hit.expireAt >= now) {
            return hit.allowed;
        }

        boolean allowed = delegate.isAllowed(request);
        cache.put(key, new CacheValue(allowed, now + ttlMillis));

        if ((accessCounter.incrementAndGet() & 0xFF) == 0) {
            cleanup(now);
        }
        return allowed;
    }

    private void cleanup(long now) {
        cache.entrySet().removeIf(entry -> entry.getValue().expireAt < now);
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private static class CacheKey {
        private final String clientId;
        private final String username;
        private final String topic;
        private final AclAction action;

        private CacheKey(AclRequest request) {
            this.clientId = normalize(request.getClientId());
            this.username = normalize(request.getUsername());
            this.topic = normalize(request.getTopic());
            this.action = request.getAction();
        }

        private static String normalize(String v) {
            return v == null ? "" : v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey cacheKey)) {
                return false;
            }
            return Objects.equals(clientId, cacheKey.clientId)
                && Objects.equals(username, cacheKey.username)
                && Objects.equals(topic, cacheKey.topic)
                && action == cacheKey.action;
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId, username, topic, action);
        }
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private static class CacheValue {
        private final boolean allowed;
        private final long expireAt;

        private CacheValue(boolean allowed, long expireAt) {
            this.allowed = allowed;
            this.expireAt = expireAt;
        }
    }
}
