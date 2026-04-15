package com.jmqx.bridge;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * 基于有界队列 + 固定线程池的异步消息桥接器。
 * 生产线程只负责快速入队；多个 worker 并行执行真正的桥接发送。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class AsyncMessageBridge implements MessageBridge {
    private static final Logger LOG = Logger.getLogger(AsyncMessageBridge.class.getName());

    private final MessageBridge delegate;
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public AsyncMessageBridge(MessageBridge delegate, int queueCapacity, int workerCount) {
        this.delegate = delegate;
        int normalizedQueueCapacity = Math.max(queueCapacity, 1024);
        int normalizedWorkerCount = Math.max(workerCount, 1);
        ThreadFactory threadFactory = bridgeThreadFactory();
        this.executor = new ThreadPoolExecutor(
            normalizedWorkerCount,
            normalizedWorkerCount,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(normalizedQueueCapacity),
            threadFactory
        );
    }

    @Override
    public void publish(BridgeMessage message) {
        if (!running.get() || message == null) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    delegate.publish(message);
                } catch (Exception exception) {
                    LOG.warning("[BRIDGE] async publish failed: " + exception.getMessage());
                }
            });
        } catch (RejectedExecutionException exception) {
            LOG.warning("[BRIDGE] async queue is full, drop message topic=" + message.topic());
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warning("[BRIDGE] async workers shutdown timeout, force stop");
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        delegate.close();
    }

    private static ThreadFactory bridgeThreadFactory() {
        AtomicInteger workerIndex = new AtomicInteger(0);
        return runnable -> {
            Thread thread = new Thread(runnable, "jmqx-bridge-worker-" + workerIndex.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }
}
