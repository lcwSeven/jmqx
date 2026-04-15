package com.jmqx.broker.qos;

import com.jmqx.broker.protocol.MqttPacketFactory;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.session.ClientSession;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.qos.Qos1InflightMessage;
import com.jmqx.store.qos.Qos1InflightStore;
import com.jmqx.store.qos.Qos2InboundInflightMessage;
import com.jmqx.store.qos.Qos2InboundState;
import com.jmqx.store.qos.Qos2InflightStore;
import com.jmqx.store.qos.Qos2OutboundInflightMessage;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * QoS inflight 状态管理器。
 * 负责 packetId 分配、QoS1/QoS2 出入站状态机、重试与重连恢复。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class BrokerInflightManager {
    private static final int MAX_PACKET_ID = 0xFFFF;
    private static final long QOS1_RETRY_INTERVAL_MS = 5000L;
    private static final int QOS1_MAX_RETRIES = 3;
    private static final long QOS2_RETRY_INTERVAL_MS = 5000L;
    private static final int QOS2_MAX_RETRIES = 5;
    private static final int RETRY_POLL_LIMIT_PER_TICK = 4096;

    private final Qos1InflightStore qos1InflightStore;
    private final Qos2InflightStore qos2InflightStore;
    private final Logger logger;
    private final ConcurrentMap<String, AtomicInteger> outboundPacketIdByClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Integer, OutboundInflightMessage>> outboundInflightByClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Integer, Qos2InboundInflightMessage>> inboundQos2ByClient = new ConcurrentHashMap<>();
    private final DelayQueue<InflightRetryTask> retryQueue = new DelayQueue<>();

    public BrokerInflightManager(Qos1InflightStore qos1InflightStore, Qos2InflightStore qos2InflightStore, Logger logger) {
        this.qos1InflightStore = qos1InflightStore == null ? Qos1InflightStore.NOOP : qos1InflightStore;
        this.qos2InflightStore = qos2InflightStore == null ? Qos2InflightStore.NOOP : qos2InflightStore;
        this.logger = logger == null ? Logger.getLogger(BrokerInflightManager.class.getName()) : logger;
    }

    public void removeRuntimeState(String clientId) {
        outboundInflightByClient.remove(clientId);
        inboundQos2ByClient.remove(clientId);
        outboundPacketIdByClient.remove(clientId);
    }

    public void clearPersistentState(String clientId) {
        qos1InflightStore.removeClient(clientId);
        qos2InflightStore.removeInboundClient(clientId);
        qos2InflightStore.removeOutboundClient(clientId);
    }

    public int nextOutboundPacketId(String clientId) {
        AtomicInteger sequence = outboundPacketIdByClient.computeIfAbsent(clientId, ignored -> new AtomicInteger(0));
        ConcurrentMap<Integer, OutboundInflightMessage> inflight = outboundInflightByClient.get(clientId);
        for (int i = 0; i < MAX_PACKET_ID; i++) {
            int next = sequence.updateAndGet(current -> current >= MAX_PACKET_ID ? 1 : current + 1);
            if (next <= 0) {
                sequence.set(1);
                continue;
            }
            if (inflight == null || !inflight.containsKey(next)) {
                return next;
            }
        }
        int fallback = sequence.get();
        logger.warning(() -> "[INFLIGHT] no free packetId for clientId=" + clientId + ", fallbackPacketId=" + fallback);
        return fallback > 0 ? fallback : 1;
    }

    public void trackInflightQos1(String clientId, int packetId, String topic, byte[] payload) {
        byte[] copy = payload == null ? new byte[0] : payload.clone();
        OutboundInflightMessage inflightMessage = OutboundInflightMessage.qos1(topic, copy, System.currentTimeMillis(), 0);
        ConcurrentMap<Integer, OutboundInflightMessage> inflight = outboundInflightByClient
                .computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>());
        putOutboundInflight(clientId, packetId, inflightMessage, inflight);
    }

    public void trackInflightQos2(String clientId, int packetId, String topic, byte[] payload) {
        byte[] copy = payload == null ? new byte[0] : payload.clone();
        ConcurrentMap<Integer, OutboundInflightMessage> inflight = outboundInflightByClient
                .computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>());
        putOutboundInflight(clientId, packetId, OutboundInflightMessage.qos2WaitPubRec(topic, copy, System.currentTimeMillis()), inflight);
    }

    public void saveInboundQos2(String clientId, int packetId, String topic, byte[] payload, boolean retain) {
        if (clientId == null || clientId.isBlank() || packetId <= 0) {
            return;
        }
        ConcurrentMap<Integer, Qos2InboundInflightMessage> pending = inboundQos2ByClient
                .computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>());
        Qos2InboundInflightMessage existing = pending.get(packetId);
        if (existing == null) {
            existing = qos2InflightStore.getInbound(clientId, packetId).orElse(null);
            if (existing != null) {
                pending.putIfAbsent(packetId, existing);
            }
        }
        if (existing != null && existing.inboundState() == Qos2InboundState.WAIT_PUBREL) {
            return;
        }
        Qos2InboundInflightMessage message = Qos2InboundInflightMessage.waitingPubRel(
                packetId,
                topic,
                payload == null ? new byte[0] : payload.clone(),
                retain
        );
        pending.put(packetId, message);
        qos2InflightStore.saveInbound(clientId, message);
    }

    public void onPubAck(String clientId, int packetId) {
        ConcurrentMap<Integer, OutboundInflightMessage> inflight = outboundInflightByClient.get(clientId);
        if (inflight == null) {
            qos1InflightStore.remove(clientId, packetId);
            return;
        }
        OutboundInflightMessage existing = inflight.get(packetId);
        if (existing == null || existing.qos() != MqttQoS.AT_LEAST_ONCE.value()) {
            return;
        }
        removeOutboundInflight(clientId, packetId, inflight);
        if (inflight.isEmpty()) {
            outboundInflightByClient.remove(clientId, inflight);
        }
    }

    public boolean onPubRec(String clientId, int packetId) {
        ConcurrentMap<Integer, OutboundInflightMessage> inflight = outboundInflightByClient.get(clientId);
        if (inflight == null) {
            return false;
        }
        OutboundInflightMessage existing = inflight.get(packetId);
        if (existing == null || existing.qos() != MqttQoS.EXACTLY_ONCE.value()) {
            return false;
        }
        if (existing.qos2State() == Qos2State.WAIT_PUBCOMP) {
            return true;
        }
        if (existing.qos2State() != Qos2State.WAIT_PUBREC) {
            return false;
        }
        OutboundInflightMessage next = existing.toQos2WaitPubComp(System.currentTimeMillis());
        putOutboundInflight(clientId, packetId, next, inflight);
        return true;
    }

    public InboundQos2ReleaseDecision onPubRel(String clientId, int packetId) {
        Qos2InboundInflightMessage inbound = getInboundQos2(clientId, packetId);
        if (inbound == null) {
            return InboundQos2ReleaseDecision.missing();
        }
        if (inbound.inboundState() == Qos2InboundState.COMPLETED) {
            return InboundQos2ReleaseDecision.completed();
        }
        return InboundQos2ReleaseDecision.process(new InboundQos2Publish(
                inbound.topic(),
                inbound.payload() == null ? new byte[0] : inbound.payload().clone(),
                inbound.retain()
        ));
    }

    public void markInboundQos2Completed(String clientId, int packetId) {
        if (clientId == null || clientId.isBlank() || packetId <= 0) {
            return;
        }
        Qos2InboundInflightMessage current = getInboundQos2(clientId, packetId);
        if (current == null || current.inboundState() == Qos2InboundState.COMPLETED) {
            return;
        }
        Qos2InboundInflightMessage completed = current.toCompleted();
        ConcurrentMap<Integer, Qos2InboundInflightMessage> pending = inboundQos2ByClient
                .computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>());
        pending.put(packetId, completed);
        qos2InflightStore.saveInbound(clientId, completed);
    }

    public void removeInboundQos2(String clientId, int packetId) {
        if (clientId == null || clientId.isBlank() || packetId <= 0) {
            return;
        }
        ConcurrentMap<Integer, Qos2InboundInflightMessage> pending = inboundQos2ByClient.get(clientId);
        if (pending != null) {
            pending.remove(packetId);
            if (pending.isEmpty()) {
                inboundQos2ByClient.remove(clientId, pending);
            }
        }
        qos2InflightStore.removeInbound(clientId, packetId);
    }

    public void onPubComp(String clientId, int packetId) {
        ConcurrentMap<Integer, OutboundInflightMessage> inflight = outboundInflightByClient.get(clientId);
        if (inflight == null) {
            return;
        }
        OutboundInflightMessage existing = inflight.get(packetId);
        if (existing == null || existing.qos() != MqttQoS.EXACTLY_ONCE.value()
                || existing.qos2State() != Qos2State.WAIT_PUBCOMP) {
            return;
        }
        removeOutboundInflight(clientId, packetId, inflight);
        if (inflight.isEmpty()) {
            outboundInflightByClient.remove(clientId, inflight);
        }
    }

    public void retryInflight(SessionRegistry sessionRegistry) {
        processDueRetryTasks(sessionRegistry);
    }

    public void restoreInflightState(String clientId, boolean sessionPresent, Channel channel) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        if (!sessionPresent) {
            clearPersistentState(clientId);
            removeRuntimeState(clientId);
            return;
        }
        restoreInboundQos2State(clientId);
        restoreOutboundInflightState(clientId, channel);
    }

    public boolean hasPersistedSessionState(String clientId, SubscriptionRegistry subscriptionRegistry) {
        if (clientId == null || clientId.isBlank()) {
            return false;
        }
        if (subscriptionRegistry != null && !subscriptionRegistry.findSubscriptions(clientId).isEmpty()) {
            return true;
        }
        if (qos1InflightStore.maxPacketId(clientId) > 0) {
            return true;
        }
        if (qos2InflightStore.maxOutboundPacketId(clientId) > 0) {
            return true;
        }
        return !qos2InflightStore.listInbound(clientId).isEmpty();
    }

    public void close() {
        qos1InflightStore.close();
        qos2InflightStore.close();
    }

    private void processDueRetryTasks(SessionRegistry sessionRegistry) {
        long now = System.currentTimeMillis();
        int processed = 0;
        while (processed < RETRY_POLL_LIMIT_PER_TICK) {
            InflightRetryTask task = retryQueue.poll();
            if (task == null) {
                return;
            }
            processed++;
            String clientId = task.clientId();
            int packetId = task.packetId();
            ConcurrentMap<Integer, OutboundInflightMessage> inflight = outboundInflightByClient.get(clientId);
            if (inflight == null) {
                continue;
            }
            OutboundInflightMessage entry = inflight.get(packetId);
            if (entry == null) {
                if (inflight.isEmpty()) {
                    outboundInflightByClient.remove(clientId, inflight);
                }
                continue;
            }
            long expectedRetryAt = entry.lastSentAtMs() + retryIntervalMs(entry);
            // 旧任务（例如状态已更新后留下的历史重试任务）直接丢弃，避免重复入队造成队列膨胀。
            if (task.retryAtMs() != expectedRetryAt) {
                continue;
            }
            if (expectedRetryAt > now) {
                retryQueue.offer(new InflightRetryTask(clientId, packetId, expectedRetryAt));
                continue;
            }
            ClientSession session = sessionRegistry == null ? null : sessionRegistry.get(clientId).orElse(null);
            Channel channel = session == null ? null : session.channel();
            if (channel == null || !channel.isActive()) {
                retryQueue.offer(new InflightRetryTask(clientId, packetId, now + retryIntervalMs(entry)));
                continue;
            }
            if (reachRetryLimit(entry)) {
                removeOutboundInflight(clientId, packetId, inflight);
                if (inflight.isEmpty()) {
                    outboundInflightByClient.remove(clientId, inflight);
                }
                continue;
            }
            if (entry.qos() == MqttQoS.AT_LEAST_ONCE.value()) {
                channel.writeAndFlush(MqttPacketFactory.buildQos1PublishMessage(entry.topic(), entry.payload(), packetId, true));
                OutboundInflightMessage retried = entry.nextQos1Retry(now);
                putOutboundInflight(clientId, packetId, retried, inflight);
            } else if (entry.qos() == MqttQoS.EXACTLY_ONCE.value()) {
                if (entry.qos2State() == Qos2State.WAIT_PUBCOMP) {
                    channel.writeAndFlush(MqttPacketFactory.buildPubRelMessage(packetId));
                } else {
                    channel.writeAndFlush(MqttPacketFactory.buildQos2PublishMessage(entry.topic(), entry.payload(), packetId, true));
                }
                OutboundInflightMessage retried = entry.nextQos2Retry(now);
                putOutboundInflight(clientId, packetId, retried, inflight);
            }
            if (inflight.isEmpty()) {
                outboundInflightByClient.remove(clientId, inflight);
            }
        }
    }

    private void restoreOutboundInflightState(String clientId, Channel channel) {
        ConcurrentMap<Integer, OutboundInflightMessage> inflight = new ConcurrentHashMap<>();
        int maxPacketId = 0;
        long now = System.currentTimeMillis();
        for (Qos1InflightMessage message : qos1InflightStore.listByClient(clientId)) {
            if (message == null || message.packetId() <= 0 || message.topic() == null || message.topic().isBlank()) {
                continue;
            }
            byte[] payload = message.payload() == null ? new byte[0] : message.payload().clone();
            OutboundInflightMessage restored = OutboundInflightMessage.qos1(
                    message.topic(), payload, now, Math.max(0, message.retryCount()));
            putOutboundInflight(clientId, message.packetId(), restored, inflight);
            maxPacketId = Math.max(maxPacketId, message.packetId());
            if (channel != null && channel.isActive()) {
                channel.writeAndFlush(MqttPacketFactory.buildQos1PublishMessage(restored.topic(), restored.payload(), message.packetId(), true));
            }
        }
        for (Qos2OutboundInflightMessage message : qos2InflightStore.listOutbound(clientId)) {
            if (message == null || message.packetId() <= 0 || message.topic() == null || message.topic().isBlank()) {
                continue;
            }
            Qos2State state = Qos2State.fromCode(message.state());
            byte[] payload = message.payload() == null ? new byte[0] : message.payload().clone();
            OutboundInflightMessage restored = OutboundInflightMessage.qos2(message.topic(), payload, state, now);
            putOutboundInflight(clientId, message.packetId(), restored, inflight);
            maxPacketId = Math.max(maxPacketId, message.packetId());
            if (channel != null && channel.isActive()) {
                if (state == Qos2State.WAIT_PUBREC) {
                    channel.writeAndFlush(MqttPacketFactory.buildQos2PublishMessage(restored.topic(), restored.payload(), message.packetId(), true));
                } else {
                    channel.writeAndFlush(MqttPacketFactory.buildPubRelMessage(message.packetId()));
                }
            }
        }
        if (inflight.isEmpty()) {
            outboundPacketIdByClient.remove(clientId);
            outboundInflightByClient.remove(clientId);
            return;
        }
        outboundInflightByClient.put(clientId, inflight);
        if (maxPacketId > 0) {
            int restoredMaxPacketId = maxPacketId;
            AtomicInteger sequence = outboundPacketIdByClient.computeIfAbsent(clientId, ignored -> new AtomicInteger(0));
            sequence.updateAndGet(current -> Math.max(current, restoredMaxPacketId));
        }
    }

    private void restoreInboundQos2State(String clientId) {
        ConcurrentMap<Integer, Qos2InboundInflightMessage> inbound = new ConcurrentHashMap<>();
        for (Qos2InboundInflightMessage message : qos2InflightStore.listInbound(clientId)) {
            if (message == null || message.packetId() <= 0 || message.topic() == null || message.topic().isBlank()) {
                continue;
            }
            inbound.put(message.packetId(), message);
        }
        if (inbound.isEmpty()) {
            inboundQos2ByClient.remove(clientId);
            return;
        }
        inboundQos2ByClient.put(clientId, inbound);
    }

    private Qos2InboundInflightMessage getInboundQos2(String clientId, int packetId) {
        if (clientId == null || clientId.isBlank() || packetId <= 0) {
            return null;
        }
        ConcurrentMap<Integer, Qos2InboundInflightMessage> pending = inboundQos2ByClient
                .computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>());
        Qos2InboundInflightMessage current = pending.get(packetId);
        if (current != null) {
            return current;
        }
        Qos2InboundInflightMessage restored = qos2InflightStore.getInbound(clientId, packetId).orElse(null);
        if (restored != null) {
            pending.putIfAbsent(packetId, restored);
            return pending.get(packetId);
        }
        if (pending.isEmpty()) {
            inboundQos2ByClient.remove(clientId, pending);
        }
        return null;
    }

    public record InboundQos2ReleaseDecision(InboundQos2Publish publish, boolean alreadyCompleted) {
        public static InboundQos2ReleaseDecision process(InboundQos2Publish publish) {
            return new InboundQos2ReleaseDecision(publish, false);
        }

        public static InboundQos2ReleaseDecision completed() {
            return new InboundQos2ReleaseDecision(null, true);
        }

        public static InboundQos2ReleaseDecision missing() {
            return new InboundQos2ReleaseDecision(null, false);
        }

        public boolean shouldProcess() {
            return publish != null;
        }

        public boolean shouldCleanupAfterAck() {
            return publish != null || alreadyCompleted;
        }
    }

    private void putOutboundInflight(
            String clientId,
            int packetId,
            OutboundInflightMessage message,
            ConcurrentMap<Integer, OutboundInflightMessage> inflight
    ) {
        if (clientId == null || clientId.isBlank() || packetId <= 0 || message == null || inflight == null) {
            return;
        }
        inflight.put(packetId, message);
        scheduleRetry(clientId, packetId, message);
        if (message.qos() == MqttQoS.AT_LEAST_ONCE.value()) {
            qos1InflightStore.save(clientId, new Qos1InflightMessage(
                    packetId,
                    message.topic(),
                    message.payload() == null ? new byte[0] : message.payload().clone(),
                    message.lastSentAtMs(),
                    Math.max(0, message.retryCount())
            ));
            qos2InflightStore.removeOutbound(clientId, packetId);
            return;
        }
        qos2InflightStore.saveOutbound(clientId, new Qos2OutboundInflightMessage(
                packetId,
                message.topic(),
                message.payload() == null ? new byte[0] : message.payload().clone(),
                message.qos2State() == null ? Qos2State.WAIT_PUBREC.code() : message.qos2State().code(),
                message.lastSentAtMs()
        ));
        qos1InflightStore.remove(clientId, packetId);
    }

    private void removeOutboundInflight(
            String clientId,
            int packetId,
            ConcurrentMap<Integer, OutboundInflightMessage> inflight
    ) {
        if (inflight != null) {
            inflight.remove(packetId);
        }
        qos1InflightStore.remove(clientId, packetId);
        qos2InflightStore.removeOutbound(clientId, packetId);
    }

    private void scheduleRetry(String clientId, int packetId, OutboundInflightMessage message) {
        if (clientId == null || clientId.isBlank() || packetId <= 0 || message == null) {
            return;
        }
        if (message.qos() != MqttQoS.AT_LEAST_ONCE.value() && message.qos() != MqttQoS.EXACTLY_ONCE.value()) {
            return;
        }
        long retryAtMs = message.lastSentAtMs() + retryIntervalMs(message);
        retryQueue.offer(new InflightRetryTask(clientId, packetId, retryAtMs));
    }

    private static long retryIntervalMs(OutboundInflightMessage message) {
        if (message == null) {
            return QOS1_RETRY_INTERVAL_MS;
        }
        return message.qos() == MqttQoS.EXACTLY_ONCE.value() ? QOS2_RETRY_INTERVAL_MS : QOS1_RETRY_INTERVAL_MS;
    }

    private static boolean reachRetryLimit(OutboundInflightMessage message) {
        if (message == null) {
            return true;
        }
        if (message.qos() == MqttQoS.EXACTLY_ONCE.value()) {
            return message.retryCount() >= QOS2_MAX_RETRIES;
        }
        return message.retryCount() >= QOS1_MAX_RETRIES;
    }

    private record InflightRetryTask(String clientId, int packetId, long retryAtMs) implements Delayed {
        @Override
        public long getDelay(TimeUnit unit) {
            long delayMs = retryAtMs - System.currentTimeMillis();
            return unit.convert(delayMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            if (other == this) {
                return 0;
            }
            long diff = getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
            if (diff == 0L) {
                return 0;
            }
            return diff < 0 ? -1 : 1;
        }
    }

}
