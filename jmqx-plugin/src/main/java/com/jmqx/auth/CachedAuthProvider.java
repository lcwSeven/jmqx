package com.jmqx.auth;

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

    public CachedAuthProvider(AuthProvider delegate, int ttlMillis) {
        this.delegate = delegate;
        this.ttlMillis = Math.max(ttlMillis, 0);
    }

    @Override
    public boolean authenticate(AuthRequest request) {
        return authenticateDecision(request) == AuthDecision.ALLOW;
    }

    @Override
    public AuthDecision authenticateDecision(AuthRequest request) {
        if (ttlMillis <= 0) {
            return delegate.authenticateDecision(request);
        }
        long now = System.currentTimeMillis();
        CacheKey key = new CacheKey(request);
        CacheValue hit = cache.get(key);
        if (hit != null && hit.expireAt >= now) {
            return hit.decision;
        }

        AuthDecision decision = delegate.authenticateDecision(request);
        cache.put(key, new CacheValue(decision, now + ttlMillis));

        if ((accessCounter.incrementAndGet() & 0xFF) == 0) {
            cleanup(now);
        }
        return decision;
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
        private final String password;

        private CacheKey(AuthRequest request) {
            this.clientId = normalize(request.getClientId());
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
            return Objects.equals(clientId, cacheKey.clientId) && Objects.equals(username, cacheKey.username) && Objects.equals(password, cacheKey.password);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId, username, password);
        }
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private static class CacheValue {
        private final AuthDecision decision;
        private final long expireAt;

        private CacheValue(AuthDecision decision, long expireAt) {
            this.decision = decision;
            this.expireAt = expireAt;
        }
    }
}
