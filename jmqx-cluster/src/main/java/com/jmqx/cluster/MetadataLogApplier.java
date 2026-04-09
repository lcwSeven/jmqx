package com.jmqx.cluster;

/**
 * 元数据日志应用器。
 * 把已提交命令按日志顺序应用到本地读模型（例如全局路由表）。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public interface MetadataLogApplier {
    void apply(long logIndex, MetadataCommand command);
}
