package com.jmqtt.auth;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class CachedAuthProvider implements AuthProvider {
    private final AuthProvider delegate;
    private final long ttlMillis;
    private final ConcurrentMap<CacheKey, CacheValue> cache = new ConcurrentHashMap<>();
    private final AtomicInteger accessCounter = new AtomicInteger();

    public CachedAuthProvider(AuthProvider delegate, int ttlSeconds) {
        this.delegate = delegate;
        this.ttlMillis = Math.max(ttlSeconds, 0) * 1000L;
    }

    @Override
    public boolean authenticate(AuthRequest request) {
        if (ttlMillis <= 0) {
            return delegate.authenticate(request);
        }
        long now = System.currentTimeMillis();
        CacheKey key = new CacheKey(request);
        CacheValue hit = cache.get(key);
        if (hit != null && hit.expireAt >= now) {
            return hit.allowed;
        }

        boolean allowed = delegate.authenticate(request);
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
        private final String username;
        private final String password;

        private CacheKey(AuthRequest request) {
            this.username = normalize(request.getUsername());
            this.password = normalize(request.getPassword());
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
            return Objects.equals(username, cacheKey.username) && Objects.equals(password, cacheKey.password);
        }

        @Override
        public int hashCode() {
            return Objects.hash(username, password);
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
