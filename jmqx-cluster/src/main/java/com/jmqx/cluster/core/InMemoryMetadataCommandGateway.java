package com.jmqx.cluster.core;

import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;
import com.jmqx.cluster.MetadataLogApplier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存版元数据命令网关。
 * 主要用于本地调试、单机验证和集群链路模拟。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class InMemoryMetadataCommandGateway implements MetadataCommandGateway {
    private final AtomicLong logIndex = new AtomicLong(0);
    private final List<MetadataLogApplier> appliers = new CopyOnWriteArrayList<>();

    public void registerApplier(MetadataLogApplier applier) {
        if (applier == null) {
            return;
        }
        appliers.add(applier);
    }

    @Override
    public long submit(MetadataCommand command) {
        if (command == null) {
            return -1L;
        }
        long next = logIndex.incrementAndGet();
        for (MetadataLogApplier applier : appliers) {
            applier.apply(next, command);
        }
        return next;
    }
}
