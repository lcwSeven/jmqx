package com.jmqx.bridge;

import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * 基于 Disruptor 的异步消息桥接器。
 * 生产线程仅负责将消息写入 RingBuffer，消费线程异步转发到具体桥接目标。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class AsyncMessageBridge implements MessageBridge {
    private static final Logger LOG = Logger.getLogger(AsyncMessageBridge.class.getName());

    private final MessageBridge delegate;
    private final Disruptor<BridgeEvent> disruptor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public AsyncMessageBridge(MessageBridge delegate, int queueCapacity, int workerCount) {
        this.delegate = delegate;
        // RingBuffer 要求容量为 2 的幂，且至少给一个基础容量，避免过小造成频繁丢弃。
        int ringBufferSize = nextPowerOfTwo(Math.max(queueCapacity, 1024));
        AtomicInteger workerIndex = new AtomicInteger(0);

        this.disruptor = new Disruptor<>(
            BridgeEvent::new,
            ringBufferSize,
            runnable -> {
                Thread thread = new Thread(runnable, "jmqx-bridge-worker-" + workerIndex.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            },
            ProducerType.MULTI,
            new YieldingWaitStrategy()
        );

        if (workerCount > 1) {
            LOG.info("[BRIDGE] disruptor currently uses single event-consumer, workerCount=" + workerCount + " will run as 1");
        }
        EventHandler<BridgeEvent> eventHandler = (event, sequence, endOfBatch) -> {
            BridgeMessage message = event.message;
            if (message == null) {
                return;
            }
            try {
                delegate.publish(message);
            } catch (Exception e) {
                LOG.warning("[BRIDGE] async publish failed: " + e.getMessage());
            } finally {
                // 及时清空引用，降低长时间运行下的对象滞留风险。
                event.message = null;
            }
        };
        this.disruptor.handleEventsWith(eventHandler);
        this.disruptor.start();
    }

    @Override
    public void publish(BridgeMessage message) {
        if (!running.get()) {
            return;
        }
        boolean published = disruptor.getRingBuffer()
            .tryPublishEvent((event, sequence, value) -> event.message = value, message);
        if (!published) {
            LOG.warning("[BRIDGE] async queue is full, drop message topic=" + message.getTopic());
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        try {
            // 尽量等待积压事件处理完成，避免服务关闭时丢失过多桥接消息。
            disruptor.shutdown(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.warning("[BRIDGE] disruptor shutdown timeout, force halt");
            disruptor.halt();
        }
        delegate.close();
    }

    private static int nextPowerOfTwo(int value) {
        int n = 1;
        while (n < value) {
            n <<= 1;
        }
        return n;
    }

    private static class BridgeEvent {
        private BridgeMessage message;
    }
}
