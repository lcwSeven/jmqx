package com.jmqx.broker.core;

/**
 * 共享订阅投递目标：
 * 1. localClientId 不为空：投递到本地客户端；
 * 2. remoteNodeId 不为空：转发到远端节点。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record SharedDeliveryTarget(String localClientId, String remoteNodeId) {
    public static SharedDeliveryTarget local(String localClientId) {
        return new SharedDeliveryTarget(localClientId, null);
    }

    public static SharedDeliveryTarget remote(String remoteNodeId) {
        return new SharedDeliveryTarget(null, remoteNodeId);
    }
}
