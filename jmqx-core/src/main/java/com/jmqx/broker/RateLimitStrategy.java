package com.jmqx.broker;

/**
 * 限流策略接口。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public interface RateLimitStrategy {
    /**
     * 尝试获取一次访问令牌。
     *
     * @return true=通过，false=被限流
     */
    boolean tryAcquire(String key, long nowMs);

    /**
     * 清理空闲 key 的状态，降低内存占用。
     */
    void cleanup(long nowMs);
}
