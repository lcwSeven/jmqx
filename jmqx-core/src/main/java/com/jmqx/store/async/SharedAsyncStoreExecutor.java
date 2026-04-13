package com.jmqx.store.async;

import java.util.Objects;

/**
 * 存储异步共享执行器。
 * 由启动层创建一次，并在多个异步存储包装器间复用，减少线程池数量。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public class SharedAsyncStoreExecutor implements AutoCloseable {
    private final AsyncStoreExecutor delegate;

    public SharedAsyncStoreExecutor(int queueCapacity, int workerCount, long enqueueTimeoutMs) {
        this("jmqx-store-async", queueCapacity, workerCount, enqueueTimeoutMs);
    }

    public SharedAsyncStoreExecutor(
            String workerNamePrefix,
            int queueCapacity,
            int workerCount,
            long enqueueTimeoutMs
    ) {
        this.delegate = new AsyncStoreExecutor(
                Objects.requireNonNullElse(workerNamePrefix, "jmqx-store-async"),
                queueCapacity,
                workerCount,
                enqueueTimeoutMs
        );
    }

    boolean submit(Runnable runnable) {
        return delegate.submit(runnable);
    }

    @Override
    public void close() {
        delegate.close();
    }
}

