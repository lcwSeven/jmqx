package com.jmqx.bridge;

import org.jetbrains.annotations.NotNull;

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
    private static final int MIN_QUEUE_CAPACITY = 1024;
    private static final int MIN_WORKER_COUNT = 1;
    private static final int SHUTDOWN_WAIT_SECONDS = 5;

    private final MessageBridge delegate;
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public AsyncMessageBridge(MessageBridge delegate, int queueCapacity, int workerCount) {
        this.delegate = delegate;
        int normalizedQueueCapacity = normalizeQueueCapacity(queueCapacity);
        int normalizedWorkerCount = normalizeWorkerCount(workerCount);
        ThreadFactory threadFactory = new BridgeThreadFactory();
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
            executor.execute(new PublishTask(delegate, message));
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
            if (!executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
                LOG.warning("[BRIDGE] async workers shutdown timeout, force stop");
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        delegate.close();
    }

    private static int normalizeQueueCapacity(int queueCapacity) {
        return Math.max(queueCapacity, MIN_QUEUE_CAPACITY);
    }

    private static int normalizeWorkerCount(int workerCount) {
        return Math.max(workerCount, MIN_WORKER_COUNT);
    }

    private static class BridgeThreadFactory implements ThreadFactory {
        private final AtomicInteger workerIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(@NotNull Runnable runnable) {
            Thread thread = new Thread(runnable, "jmqx-bridge-worker-" + workerIndex.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }

    private record PublishTask(MessageBridge delegate, BridgeMessage message) implements Runnable {

        @Override
        public void run() {
            try {
                delegate.publish(message);
            } catch (Exception exception) {
                LOG.warning("[BRIDGE] async publish failed: " + exception.getMessage());
            }
        }
    }
}
