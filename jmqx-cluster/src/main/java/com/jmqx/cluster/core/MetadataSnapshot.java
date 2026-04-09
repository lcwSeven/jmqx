package com.jmqx.cluster.core;

import com.jmqx.cluster.MetadataCommand;

import java.util.Collections;
import java.util.List;

/**
 * 元数据快照模型。
 * 用于 REPLICANT 断档过大时执行全量重建。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public record MetadataSnapshot(long baseLogIndex, List<MetadataCommand> commands) {
    public MetadataSnapshot {
        commands = commands == null ? Collections.emptyList() : List.copyOf(commands);
    }
}
