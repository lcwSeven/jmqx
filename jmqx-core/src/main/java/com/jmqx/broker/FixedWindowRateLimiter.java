package com.jmqx.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 轻量固定窗口限流器（按 key 统计每秒请求数）。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class FixedWindowRateLimiter {
    private final int limitPerSecond;
    private final long idleEvictMs;
    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();

    public FixedWindowRateLimiter(int limitPerSecond, long idleEvictMs) {
        this.limitPerSecond = Math.max(0, limitPerSecond);
        this.idleEvictMs = Math.max(1_000L, idleEvictMs);
    }

    /**
     * 尝试申请一次令牌。
     *
     * @return true=通过，false=超限
     */
    public boolean tryAcquire(String key, long nowMs) {
        if (limitPerSecond <= 0) {
            return true;
        }
        if (key == null || key.isBlank()) {
            return true;
        }
        long second = nowMs / 1000L;
        Counter counter = counters.computeIfAbsent(key, ignored -> new Counter(second, 0, nowMs));
        synchronized (counter) {
            if (counter.windowSecond != second) {
                counter.windowSecond = second;
                counter.count = 0;
            }
            counter.lastAccessMs = nowMs;
            if (counter.count >= limitPerSecond) {
                return false;
            }
            counter.count++;
            return true;
        }
    }

    /**
     * 清理长时间未访问的 key，避免 map 持续膨胀。
     */
    public void cleanup(long nowMs) {
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            Counter counter = entry.getValue();
            if (counter == null) {
                continue;
            }
            if (nowMs - counter.lastAccessMs <= idleEvictMs) {
                continue;
            }
            counters.remove(entry.getKey(), counter);
        }
    }

    private static final class Counter {
        private long windowSecond;
        private int count;
        private volatile long lastAccessMs;

        private Counter(long windowSecond, int count, long lastAccessMs) {
            this.windowSecond = windowSecond;
            this.count = count;
            this.lastAccessMs = lastAccessMs;
        }
    }
}
