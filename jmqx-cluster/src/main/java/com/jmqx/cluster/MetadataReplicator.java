package com.jmqx.cluster;

/**
 * 元数据复制组件生命周期抽象。
 * 统一管理复制通道的启动与停止。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public interface MetadataReplicator {
    void start();

    void stop();
}
