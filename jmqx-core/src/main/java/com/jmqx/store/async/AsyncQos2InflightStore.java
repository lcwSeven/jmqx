package com.jmqx.store.async;

import com.jmqx.store.qos.Qos2InboundInflightMessage;
import com.jmqx.store.qos.Qos2InflightStore;
import com.jmqx.store.qos.Qos2OutboundInflightMessage;

import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * 异步化 QoS2 inflight 存储包装器。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public class AsyncQos2InflightStore implements Qos2InflightStore {
    private static final Logger LOG = Logger.getLogger(AsyncQos2InflightStore.class.getName());

    private final Qos2InflightStore delegate;
    private final SharedAsyncStoreExecutor executor;
    private final boolean closeExecutorOnClose;

    public AsyncQos2InflightStore(Qos2InflightStore delegate, int queueCapacity, int workerCount, long enqueueTimeoutMs) {
        this.delegate = delegate == null ? Qos2InflightStore.NOOP : delegate;
        this.executor = new SharedAsyncStoreExecutor("jmqx-qos2-store", queueCapacity, workerCount, enqueueTimeoutMs);
        this.closeExecutorOnClose = true;
    }

    public AsyncQos2InflightStore(Qos2InflightStore delegate, SharedAsyncStoreExecutor sharedExecutor) {
        this.delegate = delegate == null ? Qos2InflightStore.NOOP : delegate;
        this.executor = sharedExecutor == null
                ? new SharedAsyncStoreExecutor("jmqx-qos2-store", 20000, 1, 2)
                : sharedExecutor;
        this.closeExecutorOnClose = sharedExecutor == null;
    }

    @Override
    public void saveOutbound(String clientId, Qos2OutboundInflightMessage message) {
        boolean accepted = executor.submit(() -> delegate.saveOutbound(clientId, message));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS2][ASYNC] enqueue failed, fallback sync saveOutbound");
        delegate.saveOutbound(clientId, message);
    }

    @Override
    public void removeOutbound(String clientId, int packetId) {
        boolean accepted = executor.submit(() -> delegate.removeOutbound(clientId, packetId));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS2][ASYNC] enqueue failed, fallback sync removeOutbound");
        delegate.removeOutbound(clientId, packetId);
    }

    @Override
    public void removeOutboundClient(String clientId) {
        boolean accepted = executor.submit(() -> delegate.removeOutboundClient(clientId));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS2][ASYNC] enqueue failed, fallback sync removeOutboundClient");
        delegate.removeOutboundClient(clientId);
    }

    @Override
    public List<Qos2OutboundInflightMessage> listOutbound(String clientId) {
        return delegate.listOutbound(clientId);
    }

    @Override
    public int maxOutboundPacketId(String clientId) {
        return delegate.maxOutboundPacketId(clientId);
    }

    @Override
    public void saveInbound(String clientId, Qos2InboundInflightMessage message) {
        boolean accepted = executor.submit(() -> delegate.saveInbound(clientId, message));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS2][ASYNC] enqueue failed, fallback sync saveInbound");
        delegate.saveInbound(clientId, message);
    }

    @Override
    public void removeInbound(String clientId, int packetId) {
        boolean accepted = executor.submit(() -> delegate.removeInbound(clientId, packetId));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS2][ASYNC] enqueue failed, fallback sync removeInbound");
        delegate.removeInbound(clientId, packetId);
    }

    @Override
    public void removeInboundClient(String clientId) {
        boolean accepted = executor.submit(() -> delegate.removeInboundClient(clientId));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS2][ASYNC] enqueue failed, fallback sync removeInboundClient");
        delegate.removeInboundClient(clientId);
    }

    @Override
    public Optional<Qos2InboundInflightMessage> getInbound(String clientId, int packetId) {
        return delegate.getInbound(clientId, packetId);
    }

    @Override
    public List<Qos2InboundInflightMessage> listInbound(String clientId) {
        return delegate.listInbound(clientId);
    }

    @Override
    public void close() {
        if (closeExecutorOnClose) {
            executor.close();
        }
        delegate.close();
    }
}
