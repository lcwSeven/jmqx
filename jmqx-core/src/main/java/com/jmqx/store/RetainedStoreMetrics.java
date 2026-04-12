package com.jmqx.store;

/**
 * Retained 存储运行指标快照。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public record RetainedStoreMetrics(int entries,
                                   long bytes,
                                   long evictedCount,
                                   long rejectedTooLargeCount,
                                   long rejectedCapacityCount,
                                   long updateCount) {
    public static final RetainedStoreMetrics EMPTY = new RetainedStoreMetrics(0, 0, 0, 0, 0, 0);

}
