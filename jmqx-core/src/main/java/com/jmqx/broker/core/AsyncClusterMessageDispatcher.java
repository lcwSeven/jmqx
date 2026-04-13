package com.jmqx.broker.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * 异步集群消息分发器。
 * 通过有界队列 + 工作线程隔离网络发送抖动对 MQTT 主链路的影响。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public class AsyncClusterMessageDispatcher implements ClusterMessageDispatcher, AutoCloseable {
    private static final Logger LOG = Logger.getLogger(AsyncClusterMessageDispatcher.class.getName());

    private final ClusterMessageDispatcher delegate;
    private final BlockingQueue<DispatchTask> queue;
    private final long enqueueTimeoutMs;
    private final Thread[] workers;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong droppedCount = new AtomicLong(0);
    private final AtomicLong lastDropLogAtMs = new AtomicLong(0);

    public AsyncClusterMessageDispatcher(
        ClusterMessageDispatcher delegate,
        int queueCapacity,
        int workerCount,
        long enqueueTimeoutMs
    ) {
        this.delegate = delegate == null ? (topic, payload, publishQos, targetPlans) -> { } : delegate;
        this.queue = new ArrayBlockingQueue<>(Math.max(1_024, queueCapacity));
        this.enqueueTimeoutMs = Math.max(0L, enqueueTimeoutMs);
        int normalizedWorkerCount = Math.max(1, workerCount);
        this.workers = new Thread[normalizedWorkerCount];
        for (int i = 0; i < normalizedWorkerCount; i++) {
            int index = i;
            Thread worker = new Thread(() -> runWorker(index), "jmqx-cluster-dispatch-" + index);
            worker.setDaemon(true);
            worker.start();
            workers[i] = worker;
        }
    }

    @Override
    public void dispatch(String topic, byte[] payload, int publishQos, Map<String, DispatchTarget> targetPlans) {
        if (!running.get() || topic == null || topic.isBlank() || payload == null || targetPlans == null || targetPlans.isEmpty()) {
            return;
        }
        DispatchTask task = new DispatchTask(topic, payload, publishQos, copyTargetPlans(targetPlans));
        boolean offered;
        try {
            offered = enqueueTimeoutMs == 0
                ? queue.offer(task)
                : queue.offer(task, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return;
        }
        if (offered) {
            return;
        }
        long dropped = droppedCount.incrementAndGet();
        logDropIfDue(dropped, topic);
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        for (Thread worker : workers) {
            if (worker != null) {
                worker.interrupt();
            }
        }
        for (Thread worker : workers) {
            if (worker == null) {
                continue;
            }
            try {
                worker.join(1000L);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        queue.clear();
    }

    private void runWorker(int index) {
        while (running.get()) {
            try {
                DispatchTask task = queue.poll(500, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                delegate.dispatch(task.topic(), task.payload(), task.publishQos(), task.targetPlans());
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception exception) {
                LOG.warning("[CLUSTER][ASYNC_DISPATCH] worker failed index=" + index + ", error=" + exception.getMessage());
            }
        }
    }

    private Map<String, DispatchTarget> copyTargetPlans(Map<String, DispatchTarget> targetPlans) {
        Map<String, DispatchTarget> copied = new HashMap<>(targetPlans.size());
        targetPlans.forEach((nodeId, target) -> {
            if (nodeId == null || nodeId.isBlank() || target == null) {
                return;
            }
            copied.put(nodeId, target);
        });
        return copied;
    }

    private void logDropIfDue(long dropped, String topic) {
        long now = System.currentTimeMillis();
        long last = lastDropLogAtMs.get();
        if (now - last < 1000) {
            return;
        }
        if (!lastDropLogAtMs.compareAndSet(last, now)) {
            return;
        }
        LOG.warning(() -> "[CLUSTER][ASYNC_DISPATCH] queue full, drop message"
            + ", droppedTotal=" + dropped + ", queueSize=" + queue.size() + ", topic=" + topic);
    }

    private record DispatchTask(
        String topic,
        byte[] payload,
        int publishQos,
        Map<String, DispatchTarget> targetPlans
    ) {
    }
}

