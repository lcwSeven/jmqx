package com.jmqx.store.async;

import com.jmqx.store.qos.Qos1InflightMessage;
import com.jmqx.store.qos.Qos1InflightStore;

import java.util.List;
import java.util.logging.Logger;

/**
 * 异步化 QoS1 inflight 存储包装器。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public class AsyncQos1InflightStore implements Qos1InflightStore {
    private static final Logger LOG = Logger.getLogger(AsyncQos1InflightStore.class.getName());

    private final Qos1InflightStore delegate;
    private final SharedAsyncStoreExecutor executor;
    private final boolean closeExecutorOnClose;

    public AsyncQos1InflightStore(Qos1InflightStore delegate, int queueCapacity, int workerCount, long enqueueTimeoutMs) {
        this.delegate = delegate == null ? Qos1InflightStore.NOOP : delegate;
        this.executor = new SharedAsyncStoreExecutor("jmqx-qos1-store", queueCapacity, workerCount, enqueueTimeoutMs);
        this.closeExecutorOnClose = true;
    }

    public AsyncQos1InflightStore(Qos1InflightStore delegate, SharedAsyncStoreExecutor sharedExecutor) {
        this.delegate = delegate == null ? Qos1InflightStore.NOOP : delegate;
        this.executor = sharedExecutor == null
                ? new SharedAsyncStoreExecutor("jmqx-qos1-store", 20000, 1, 2)
                : sharedExecutor;
        this.closeExecutorOnClose = sharedExecutor == null;
    }

    @Override
    public void save(String clientId, Qos1InflightMessage message) {
        boolean accepted = executor.submit(() -> delegate.save(clientId, message));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS1][ASYNC] enqueue failed, fallback sync save");
        delegate.save(clientId, message);
    }

    @Override
    public void remove(String clientId, int packetId) {
        boolean accepted = executor.submit(() -> delegate.remove(clientId, packetId));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS1][ASYNC] enqueue failed, fallback sync remove");
        delegate.remove(clientId, packetId);
    }

    @Override
    public void removeClient(String clientId) {
        boolean accepted = executor.submit(() -> delegate.removeClient(clientId));
        if (accepted) {
            return;
        }
        LOG.warning("[QOS1][ASYNC] enqueue failed, fallback sync removeClient");
        delegate.removeClient(clientId);
    }

    @Override
    public List<Qos1InflightMessage> listByClient(String clientId) {
        return delegate.listByClient(clientId);
    }

    @Override
    public int maxPacketId(String clientId) {
        return delegate.maxPacketId(clientId);
    }

    @Override
    public void close() {
        if (closeExecutorOnClose) {
            executor.close();
        }
        delegate.close();
    }
}
