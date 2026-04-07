package com.jmqx.store;

/**
 * Retained 存储运行指标快照。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class RetainedStoreMetrics {
    public static final RetainedStoreMetrics EMPTY = new RetainedStoreMetrics(0, 0, 0, 0, 0, 0);

    private final int entries;
    private final long bytes;
    private final long evictedCount;
    private final long rejectedTooLargeCount;
    private final long rejectedCapacityCount;
    private final long updateCount;

    public RetainedStoreMetrics(
        int entries,
        long bytes,
        long evictedCount,
        long rejectedTooLargeCount,
        long rejectedCapacityCount,
        long updateCount
    ) {
        this.entries = entries;
        this.bytes = bytes;
        this.evictedCount = evictedCount;
        this.rejectedTooLargeCount = rejectedTooLargeCount;
        this.rejectedCapacityCount = rejectedCapacityCount;
        this.updateCount = updateCount;
    }

    public int getEntries() {
        return entries;
    }

    public long getBytes() {
        return bytes;
    }

    public long getEvictedCount() {
        return evictedCount;
    }

    public long getRejectedTooLargeCount() {
        return rejectedTooLargeCount;
    }

    public long getRejectedCapacityCount() {
        return rejectedCapacityCount;
    }

    public long getUpdateCount() {
        return updateCount;
    }
}
