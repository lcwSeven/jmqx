package com.jmqx.broker;

/**
 * MQTT Broker 连接/发布限流控制器。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class BrokerRateLimiter {
    private final FixedWindowRateLimiter publishClientIdRateLimiter;
    private final FixedWindowRateLimiter publishIpRateLimiter;
    private final FixedWindowRateLimiter connectGlobalRateLimiter;
    private final FixedWindowRateLimiter connectIpRateLimiter;
    private final boolean publishClientIdRateLimitEnabled;
    private final boolean publishIpRateLimitEnabled;
    private final boolean connectRateLimitEnabled;
    private final long cleanupIntervalMs;
    private volatile long nextCleanupAtMs;

    public BrokerRateLimiter(BrokerRateLimitConfig config) {
        BrokerRateLimitConfig effective = config == null
                ? BrokerRateLimitConfig.of(false, 0, false, 0, false, 0, 0, 60, 300)
                : config;
        this.publishClientIdRateLimitEnabled = effective.publishClientIdEnabled() && effective.publishClientIdPerSecond() > 0;
        this.publishIpRateLimitEnabled = effective.publishIpEnabled() && effective.publishIpPerSecond() > 0;
        this.connectRateLimitEnabled = effective.connectEnabled()
                && (effective.connectGlobalPerSecond() > 0 || effective.connectIpPerSecond() > 0);
        long idleMs = Math.max(1, effective.idleSeconds()) * 1000L;
        this.publishClientIdRateLimiter = new FixedWindowRateLimiter(effective.publishClientIdPerSecond(), idleMs);
        this.publishIpRateLimiter = new FixedWindowRateLimiter(effective.publishIpPerSecond(), idleMs);
        this.connectGlobalRateLimiter = new FixedWindowRateLimiter(effective.connectGlobalPerSecond(), idleMs);
        this.connectIpRateLimiter = new FixedWindowRateLimiter(effective.connectIpPerSecond(), idleMs);
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
}
