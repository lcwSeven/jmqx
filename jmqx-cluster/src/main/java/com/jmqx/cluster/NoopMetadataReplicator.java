package com.jmqx.cluster;

/**
 * 单机模式下的空复制器实现。
 * 保持生命周期调用兼容，但不建立任何复制通道。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class NoopMetadataReplicator implements MetadataReplicator {
    public static final NoopMetadataReplicator INSTANCE = new NoopMetadataReplicator();

    private NoopMetadataReplicator() {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
