package com.jmqx.broker;

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
        boolean connectEnabled,
        int connectGlobalPerSecond,
        int connectIpPerSecond,
        int cleanupIntervalSeconds,
        int idleSeconds
) {
    public static BrokerRateLimitConfig of(
            boolean publishClientIdEnabled,
            int publishClientIdPerSecond,
            boolean publishIpEnabled,
            int publishIpPerSecond,
            boolean connectEnabled,
            int connectGlobalPerSecond,
            int connectIpPerSecond,
            int cleanupIntervalSeconds,
            int idleSeconds
    ) {
        return new BrokerRateLimitConfig(
                publishClientIdEnabled,
                publishClientIdPerSecond,
                publishIpEnabled,
                publishIpPerSecond,
                connectEnabled,
                connectGlobalPerSecond,
                connectIpPerSecond,
                cleanupIntervalSeconds,
                idleSeconds
        );
    }
}
