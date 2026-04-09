package com.jmqx.cluster;

/**
 * 单机模式下的空实现命令网关。
 * 返回 -1 表示未真正提交到集群元数据日志。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class NoopMetadataCommandGateway implements MetadataCommandGateway {
    public static final NoopMetadataCommandGateway INSTANCE = new NoopMetadataCommandGateway();

    private NoopMetadataCommandGateway() {
    }

    @Override
    public long submit(MetadataCommand command) {
        return -1L;
    }
}
