package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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
    public AuthResult authenticateResult(AuthRequest request) {
        if (ttlMillis <= 0) {
            return delegate.authenticateResult(request);
        }
        long now = System.currentTimeMillis();
        CacheKey key = new CacheKey(request);
        CacheValue hit = cache.get(key);
        if (hit != null && hit.expireAt >= now) {
            return hit.result;
        }

        AuthResult result = delegate.authenticateResult(request);
        cache.put(key, new CacheValue(result, now + ttlMillis));

        if ((accessCounter.incrementAndGet() & 0xFF) == 0) {
            cleanup(now);
        }
        return result;
    }

    @Override
    public CompletableFuture<AuthResult> authenticateAsync(AuthRequest request) {
        if (ttlMillis <= 0) {
            return delegate.authenticateAsync(request);
        }
        long now = System.currentTimeMillis();
        CacheKey key = new CacheKey(request);
        CacheValue hit = cache.get(key);
        if (hit != null && hit.expireAt >= now) {
            return CompletableFuture.completedFuture(hit.result);
        }
        return delegate.authenticateAsync(request).thenApply(result -> {
            cache.put(key, new CacheValue(result, now + ttlMillis));
            if ((accessCounter.incrementAndGet() & 0xFF) == 0) {
                cleanup(System.currentTimeMillis());
            }
            return result;
        });
    }

    private void cleanup(long now) {
        cache.entrySet().removeIf(entry -> entry.getValue().expireAt < now);
    }

    @Override
    public void evictCache(String clientId, String username) {
        String normalizedClientId = CacheKey.normalize(clientId);
        if (normalizedClientId.isEmpty()) {
            return;
        }
        cache.remove(new CacheKey(normalizedClientId));
        delegate.evictCache(clientId, username);
    }

    @Override
    public void close() {
        cache.clear();
        delegate.close();
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private static class CacheKey {
        private final String clientId;

        private CacheKey(AuthRequest request) {
            this(normalize(request.getClientId()));
        }

        private CacheKey(String clientId) {
            this.clientId = normalize(clientId);
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
            return Objects.equals(clientId, cacheKey.clientId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId);
        }
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private static class CacheValue {
        private final AuthResult result;
        private final long expireAt;

        private CacheValue(AuthResult result, long expireAt) {
            this.result = result;
            this.expireAt = expireAt;
        }
    }
}
