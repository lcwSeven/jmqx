package com.jmqx.store;

import java.util.Locale;

/**
 * Retained 容量溢出策略。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public enum RetainedOverflowStrategy {
    // 移除最不常用的
    EVICT_LRU,
    // 拒绝新消息
    REJECT_NEW;

    public static RetainedOverflowStrategy parse(String raw, RetainedOverflowStrategy defaultValue) {
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "evict_lru", "evict-lru", "evictlru" -> EVICT_LRU;
            case "reject_new", "reject-new", "rejectnew", "reject" -> REJECT_NEW;
            default -> defaultValue;
        };
    }
}
