package com.jmqx.broker;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 滑动窗口限流器（按 key 统计最近 1 秒请求数）。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class SlidingWindowRateLimiter implements RateLimitStrategy {
    private static final long WINDOW_MS = 1000L;

    private final int limitPerSecond;
    private final long idleEvictMs;
    private final ConcurrentHashMap<String, SlidingWindowCounter> counters = new ConcurrentHashMap<>();

    public SlidingWindowRateLimiter(int limitPerSecond, long idleEvictMs) {
        this.limitPerSecond = Math.max(0, limitPerSecond);
        this.idleEvictMs = Math.max(WINDOW_MS, idleEvictMs);
    }

    @Override
    public boolean tryAcquire(String key, long nowMs) {
        if (limitPerSecond <= 0 || key == null || key.isBlank()) {
            return true;
        }
        SlidingWindowCounter counter = counters.computeIfAbsent(key, ignored -> new SlidingWindowCounter(nowMs));
        synchronized (counter) {
            long cutoff = nowMs - WINDOW_MS;
            while (!counter.timestamps.isEmpty() && counter.timestamps.peekFirst() <= cutoff) {
                counter.timestamps.pollFirst();
            }
            counter.lastAccessMs = nowMs;
            if (counter.timestamps.size() >= limitPerSecond) {
                return false;
            }
            counter.timestamps.offerLast(nowMs);
            return true;
        }
    }

    @Override
    public void cleanup(long nowMs) {
        for (Map.Entry<String, SlidingWindowCounter> entry : counters.entrySet()) {
            SlidingWindowCounter counter = entry.getValue();
            if (counter == null) {
                continue;
            }
            if (nowMs - counter.lastAccessMs <= idleEvictMs) {
                continue;
            }
            counters.remove(entry.getKey(), counter);
        }
    }

    private static final class SlidingWindowCounter {
        private final ArrayDeque<Long> timestamps = new ArrayDeque<>();
        private volatile long lastAccessMs;

        private SlidingWindowCounter(long nowMs) {
            this.lastAccessMs = nowMs;
        }
    }
}
