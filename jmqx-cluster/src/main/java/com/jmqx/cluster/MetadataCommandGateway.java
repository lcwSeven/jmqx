package com.jmqx.cluster;

/**
 * 元数据命令提交网关。
 * 返回值为已提交的日志索引，用于后续重放与断点续传。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public interface MetadataCommandGateway {
    long submit(MetadataCommand command);
}
