package com.jmqx.broker.ratelimit;

/**
 * Broker 限流配置快照。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record BrokerRateLimitConfig(
        boolean publishClientIdEnabled,
        int publishClientIdPerSecond,
        boolean publishIpEnabled,
        int publishIpPerSecond,
        String publishStrategy,
        boolean connectEnabled,
        int connectGlobalPerSecond,
        int connectIpPerSecond,
        String connectStrategy,
        int cleanupIntervalSeconds,
        int idleSeconds
) {
    public static BrokerRateLimitConfig of(
            boolean publishClientIdEnabled,
            int publishClientIdPerSecond,
            boolean publishIpEnabled,
            int publishIpPerSecond,
            String publishStrategy,
            boolean connectEnabled,
            int connectGlobalPerSecond,
            int connectIpPerSecond,
            String connectStrategy,
            int cleanupIntervalSeconds,
            int idleSeconds
    ) {
        return new BrokerRateLimitConfig(
                publishClientIdEnabled,
                publishClientIdPerSecond,
                publishIpEnabled,
                publishIpPerSecond,
                publishStrategy,
                connectEnabled,
                connectGlobalPerSecond,
                connectIpPerSecond,
                connectStrategy,
                cleanupIntervalSeconds,
                idleSeconds
        );
    }
}
