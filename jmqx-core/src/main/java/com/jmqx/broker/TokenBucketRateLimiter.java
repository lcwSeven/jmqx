package com.jmqx.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 令牌桶限流器（按 key 独立桶，桶容量=每秒速率）。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class TokenBucketRateLimiter implements RateLimitStrategy {
    private final int ratePerSecond;
    private final int capacity;
    private final long idleEvictMs;
    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    public TokenBucketRateLimiter(int ratePerSecond, long idleEvictMs) {
        this.ratePerSecond = Math.max(0, ratePerSecond);
        this.capacity = Math.max(1, this.ratePerSecond);
        this.idleEvictMs = Math.max(1_000L, idleEvictMs);
    }

    @Override
    public boolean tryAcquire(String key, long nowMs) {
        if (ratePerSecond <= 0 || key == null || key.isBlank()) {
            return true;
        }
        TokenBucket bucket = buckets.computeIfAbsent(key, ignored -> new TokenBucket(capacity, nowMs));
        synchronized (bucket) {
            refill(bucket, nowMs);
            bucket.lastAccessMs = nowMs;
            if (bucket.tokens < 1.0D) {
                return false;
            }
            bucket.tokens -= 1.0D;
            return true;
        }
    }

    @Override
    public void cleanup(long nowMs) {
        for (Map.Entry<String, TokenBucket> entry : buckets.entrySet()) {
            TokenBucket bucket = entry.getValue();
            if (bucket == null) {
                continue;
            }
            if (nowMs - bucket.lastAccessMs <= idleEvictMs) {
                continue;
            }
            buckets.remove(entry.getKey(), bucket);
        }
    }

    private void refill(TokenBucket bucket, long nowMs) {
        long elapsedMs = nowMs - bucket.lastRefillMs;
        if (elapsedMs <= 0) {
            return;
        }
        double refillTokens = (elapsedMs * ratePerSecond) / 1000.0D;
        bucket.tokens = Math.min(capacity, bucket.tokens + refillTokens);
        bucket.lastRefillMs = nowMs;
    }

    private static final class TokenBucket {
        private double tokens;
        private long lastRefillMs;
        private volatile long lastAccessMs;

        private TokenBucket(int capacity, long nowMs) {
            this.tokens = capacity;
            this.lastRefillMs = nowMs;
            this.lastAccessMs = nowMs;
        }
    }
}
