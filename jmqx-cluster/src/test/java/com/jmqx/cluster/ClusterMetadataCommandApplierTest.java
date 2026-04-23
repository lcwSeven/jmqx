package com.jmqx.cluster;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterMetadataCommandApplierTest {
    @Test
    void shouldDispatchToMatchingNamespaceHandler() {
        AtomicInteger routeCalls = new AtomicInteger();
        AtomicInteger sessionCalls = new AtomicInteger();
        AtomicInteger retainedCalls = new AtomicInteger();

        ClusterMetadataCommandApplier applier = new ClusterMetadataCommandApplier(
            "core-1",
            (logIndex, command) -> routeCalls.incrementAndGet(),
            (localNodeId, command) -> sessionCalls.incrementAndGet(),
            (logIndex, localNodeId, command) -> retainedCalls.incrementAndGet(),
            (localNodeId, command) -> { },
            (localNodeId, command) -> { },
            (localNodeId, command) -> { },
            (localNodeId, command) -> { },
            (localNodeId, command) -> { }
        );

        applier.apply(10L, new MetadataCommand(ClusterMetadataCommandApplier.SUBSCRIPTION_NAMESPACE, "upsert", "a", "b", "core-1"));
        applier.apply(11L, new MetadataCommand(ClusterMetadataCommandApplier.SESSION_NAMESPACE, "online", "a", "b", "core-1"));
        applier.apply(12L, new MetadataCommand(ClusterMetadataCommandApplier.RETAINED_NAMESPACE, "put", "a", "b", "core-1"));

        assertEquals(1, routeCalls.get());
        assertEquals(1, sessionCalls.get());
        assertEquals(1, retainedCalls.get());
    }

    @Test
    void shouldIgnoreUnknownNamespace() {
        AtomicInteger routeCalls = new AtomicInteger();

        ClusterMetadataCommandApplier applier = new ClusterMetadataCommandApplier(
            "core-1",
            (logIndex, command) -> routeCalls.incrementAndGet(),
            (localNodeId, command) -> { },
            (logIndex, localNodeId, command) -> { },
            (localNodeId, command) -> { },
            (localNodeId, command) -> { },
            (localNodeId, command) -> { },
            (localNodeId, command) -> { },
            (localNodeId, command) -> { }
        );

        applier.apply(10L, new MetadataCommand("unknown.namespace", "noop", "a", "b", "core-1"));

        assertEquals(0, routeCalls.get());
    }
}
