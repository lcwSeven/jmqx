package com.jmqx.store.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * 异步存储执行器。
 * 使用有界队列 + 固定工作线程执行存储写操作，避免协议线程被磁盘 IO 拖慢。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
final class AsyncStoreExecutor implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(AsyncStoreExecutor.class.getName());
    private static final int MAX_DRAIN_BATCH = 256;

    private final BlockingQueue<Runnable> queue;
    private final long enqueueTimeoutMs;
    private final List<Thread> workers;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ThreadLocal<List<Runnable>> drainBufferHolder = ThreadLocal.withInitial(
        () -> new ArrayList<>(MAX_DRAIN_BATCH)
    );

    AsyncStoreExecutor(String workerNamePrefix, int queueCapacity, int workerCount, long enqueueTimeoutMs) {
        this.queue = new ArrayBlockingQueue<>(Math.max(1024, queueCapacity));
        this.enqueueTimeoutMs = Math.max(0L, enqueueTimeoutMs);
        this.workers = new ArrayList<>(Math.max(1, workerCount));
        int normalizedWorkers = Math.max(1, workerCount);
        for (int i = 0; i < normalizedWorkers; i++) {
            Thread worker = new Thread(this::runLoop, workerNamePrefix + "-" + i);
            worker.setDaemon(true);
            worker.start();
            workers.add(worker);
        }
    }

    boolean submit(Runnable runnable) {
        if (runnable == null || !running.get()) {
            return false;
        }
        try {
            if (enqueueTimeoutMs == 0L) {
                return queue.offer(runnable);
            }
            return queue.offer(runnable, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        for (Thread worker : workers) {
            worker.interrupt();
        }
        for (Thread worker : workers) {
            try {
                worker.join(1000L);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Runnable task;
        while ((task = queue.poll()) != null) {
            runTask(task);
        }
    }

    private void runLoop() {
        while (running.get()) {
            try {
                Runnable task = queue.poll(500, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                runTask(task);
                List<Runnable> drainBuffer = drainBufferHolder.get();
                drainBuffer.clear();
                queue.drainTo(drainBuffer, MAX_DRAIN_BATCH);
                for (Runnable drainedTask : drainBuffer) {
                    runTask(drainedTask);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void runTask(Runnable task) {
        try {
            task.run();
        } catch (Exception exception) {
            LOG.warning("[STORE][ASYNC] task failed, error=" + exception.getMessage());
        }
    }
}
