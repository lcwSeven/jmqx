package com.jmqx.store.async;

import com.jmqx.broker.core.WillMessage;
import com.jmqx.store.will.WillMessageStore;

import java.util.Optional;
import java.util.logging.Logger;

/**
 * 异步化遗嘱存储包装器。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public class AsyncWillMessageStore implements WillMessageStore {
    private static final Logger LOG = Logger.getLogger(AsyncWillMessageStore.class.getName());

    private final WillMessageStore delegate;
    private final SharedAsyncStoreExecutor executor;
    private final boolean closeExecutorOnClose;

    public AsyncWillMessageStore(WillMessageStore delegate, int queueCapacity, int workerCount, long enqueueTimeoutMs) {
        this.delegate = delegate == null ? WillMessageStore.NOOP : delegate;
        this.executor = new SharedAsyncStoreExecutor("jmqx-will-store", queueCapacity, workerCount, enqueueTimeoutMs);
        this.closeExecutorOnClose = true;
    }

    public AsyncWillMessageStore(WillMessageStore delegate, SharedAsyncStoreExecutor sharedExecutor) {
        this.delegate = delegate == null ? WillMessageStore.NOOP : delegate;
        this.executor = sharedExecutor == null
                ? new SharedAsyncStoreExecutor("jmqx-will-store", 20000, 1, 2)
                : sharedExecutor;
        this.closeExecutorOnClose = sharedExecutor == null;
    }

    @Override
    public void save(String clientId, WillMessage willMessage) {
        boolean accepted = executor.submit(() -> delegate.save(clientId, willMessage));
        if (accepted) {
            return;
        }
        LOG.warning("[WILL][ASYNC] enqueue failed, fallback sync save");
        delegate.save(clientId, willMessage);
    }

    @Override
    public Optional<WillMessage> get(String clientId) {
        return delegate.get(clientId);
    }

    @Override
    public void remove(String clientId) {
        boolean accepted = executor.submit(() -> delegate.remove(clientId));
        if (accepted) {
            return;
        }
        LOG.warning("[WILL][ASYNC] enqueue failed, fallback sync remove");
        delegate.remove(clientId);
    }

    @Override
    public void close() {
        if (closeExecutorOnClose) {
            executor.close();
        }
        delegate.close();
    }
}
