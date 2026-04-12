package com.jmqx.broker;

/**
 * MQTT Broker 连接/发布限流控制器。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class BrokerRateLimiter {
    private static final String STRATEGY_FIXED_WINDOW = "fixed_window";
    private static final String STRATEGY_SLIDING_WINDOW = "sliding_window";
    private static final String STRATEGY_TOKEN_BUCKET = "token_bucket";

    private final RateLimitStrategy publishClientIdRateLimiter;
    private final RateLimitStrategy publishIpRateLimiter;
    private final RateLimitStrategy connectGlobalRateLimiter;
    private final RateLimitStrategy connectIpRateLimiter;
    private final boolean publishClientIdRateLimitEnabled;
    private final boolean publishIpRateLimitEnabled;
    private final boolean connectRateLimitEnabled;
    private final long cleanupIntervalMs;
    private volatile long nextCleanupAtMs;

    public BrokerRateLimiter(BrokerRateLimitConfig config) {
        BrokerRateLimitConfig effective = config == null
                ? BrokerRateLimitConfig.of(false, 0, false, 0, STRATEGY_FIXED_WINDOW, false, 0, 0, STRATEGY_FIXED_WINDOW, 60, 300)
                : config;
        this.publishClientIdRateLimitEnabled = effective.publishClientIdEnabled() && effective.publishClientIdPerSecond() > 0;
        this.publishIpRateLimitEnabled = effective.publishIpEnabled() && effective.publishIpPerSecond() > 0;
        this.connectRateLimitEnabled = effective.connectEnabled()
                && (effective.connectGlobalPerSecond() > 0 || effective.connectIpPerSecond() > 0);
        long idleMs = Math.max(1, effective.idleSeconds()) * 1000L;
        String publishStrategy = normalizeStrategy(effective.publishStrategy());
        String connectStrategy = normalizeStrategy(effective.connectStrategy());
        this.publishClientIdRateLimiter = createStrategy(publishStrategy, effective.publishClientIdPerSecond(), idleMs);
        this.publishIpRateLimiter = createStrategy(publishStrategy, effective.publishIpPerSecond(), idleMs);
        this.connectGlobalRateLimiter = createStrategy(connectStrategy, effective.connectGlobalPerSecond(), idleMs);
        this.connectIpRateLimiter = createStrategy(connectStrategy, effective.connectIpPerSecond(), idleMs);
        this.cleanupIntervalMs = Math.max(1, effective.cleanupIntervalSeconds()) * 1000L;
        this.nextCleanupAtMs = System.currentTimeMillis() + this.cleanupIntervalMs;
    }

    /**
     * 检查 PUBLISH 是否触发限流。
     *
     * @return 命中类型（clientId/ip），未命中返回 null
     */
    public String checkPublish(String clientId, String clientIp, long nowMs) {
        if (publishClientIdRateLimitEnabled && !publishClientIdRateLimiter.tryAcquire(clientId, nowMs)) {
            return "clientId";
        }
        if (publishIpRateLimitEnabled && !publishIpRateLimiter.tryAcquire(clientIp, nowMs)) {
            return "ip";
        }
        return null;
    }

    /**
     * 检查 CONNECT 是否触发限流。
     *
     * @return 命中类型（global/ip），未命中返回 null
     */
    public String checkConnect(String clientIp, long nowMs) {
        if (!connectRateLimitEnabled) {
            return null;
        }
        if (!connectGlobalRateLimiter.tryAcquire("__global__", nowMs)) {
            return "global";
        }
        if (!connectIpRateLimiter.tryAcquire(clientIp, nowMs)) {
            return "ip";
        }
        return null;
    }

    /**
     * 按周期清理限流状态。
     */
    public void cleanupIfDue(long nowMs) {
        if (nowMs < nextCleanupAtMs) {
            return;
        }
        if (publishClientIdRateLimitEnabled) {
            publishClientIdRateLimiter.cleanup(nowMs);
        }
        if (publishIpRateLimitEnabled) {
            publishIpRateLimiter.cleanup(nowMs);
        }
        if (connectRateLimitEnabled) {
            connectGlobalRateLimiter.cleanup(nowMs);
            connectIpRateLimiter.cleanup(nowMs);
        }
        nextCleanupAtMs = nowMs + cleanupIntervalMs;
    }

    private static RateLimitStrategy createStrategy(String strategy, int limitPerSecond, long idleMs) {
        return switch (strategy) {
            case STRATEGY_SLIDING_WINDOW -> new SlidingWindowRateLimiter(limitPerSecond, idleMs);
            case STRATEGY_TOKEN_BUCKET -> new TokenBucketRateLimiter(limitPerSecond, idleMs);
            default -> new FixedWindowRateLimiter(limitPerSecond, idleMs);
        };
    }

    private static String normalizeStrategy(String strategy) {
        if (strategy == null || strategy.isBlank()) {
            return STRATEGY_FIXED_WINDOW;
        }
        String normalized = strategy.trim().toLowerCase().replace('-', '_');
        if (STRATEGY_FIXED_WINDOW.equals(normalized)
                || STRATEGY_SLIDING_WINDOW.equals(normalized)
                || STRATEGY_TOKEN_BUCKET.equals(normalized)) {
            return normalized;
        }
        return STRATEGY_FIXED_WINDOW;
    }
}
