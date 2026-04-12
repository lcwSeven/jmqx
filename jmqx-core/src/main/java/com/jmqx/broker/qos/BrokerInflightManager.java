package com.jmqx.broker.qos;

import com.jmqx.broker.protocol.MqttPacketFactory;
import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.session.ClientSession;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.Qos1InflightMessage;
import com.jmqx.store.Qos1InflightStore;
import com.jmqx.store.Qos2InboundInflightMessage;
import com.jmqx.store.Qos2InflightStore;
import com.jmqx.store.Qos2OutboundInflightMessage;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    private final Qos1InflightStore qos1InflightStore;
    private final Qos2InflightStore qos2InflightStore;
    private final Logger logger;
    private final ConcurrentMap<String, AtomicInteger> outboundPacketIdByClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Integer, OutboundInflightMessage>> outboundInflightByClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Integer, InboundQos2Publish>> inboundQos2ByClient = new ConcurrentHashMap<>();

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
        ConcurrentMap<Integer, InboundQos2Publish> pending = inboundQos2ByClient
                .computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>());
        pending.computeIfAbsent(packetId, ignored -> new InboundQos2Publish(topic, payload.clone(), retain));
        qos2InflightStore.saveInbound(clientId, new Qos2InboundInflightMessage(
                packetId,
                topic,
                payload.clone(),
                retain
        ));
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

    public InboundQos2Publish onPubRel(String clientId, int packetId) {
        ConcurrentMap<Integer, InboundQos2Publish> pending = inboundQos2ByClient.get(clientId);
        InboundQos2Publish publish = pending == null ? null : pending.remove(packetId);
        if (publish == null) {
            publish = qos2InflightStore.getInbound(clientId, packetId)
                    .map(message -> new InboundQos2Publish(
                            message.topic(),
                            message.payload() == null ? new byte[0] : message.payload().clone(),
                            message.retain()
                    ))
                    .orElse(null);
        }
        if (pending != null && pending.isEmpty()) {
            inboundQos2ByClient.remove(clientId, pending);
        }
        qos2InflightStore.removeInbound(clientId, packetId);
        return publish;
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
        retryInflightQos1Messages(sessionRegistry);
        retryInflightQos2Messages(sessionRegistry);
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

    private void retryInflightQos1Messages(SessionRegistry sessionRegistry) {
        long now = System.currentTimeMillis();
        outboundInflightByClient.forEach((clientId, inflight) -> {
            ClientSession session = sessionRegistry == null ? null : sessionRegistry.get(clientId).orElse(null);
            if (session == null || session.channel() == null || !session.channel().isActive()) {
                return;
            }
            Channel channel = session.channel();
            inflight.forEach((packetId, entry) -> {
                if (entry.qos() != MqttQoS.AT_LEAST_ONCE.value()) {
                    return;
                }
                if (now - entry.lastSentAtMs() < QOS1_RETRY_INTERVAL_MS) {
                    return;
                }
                if (entry.retryCount() >= QOS1_MAX_RETRIES) {
                    removeOutboundInflight(clientId, packetId, inflight);
                    return;
                }
                channel.writeAndFlush(MqttPacketFactory.buildQos1PublishMessage(entry.topic(), entry.payload(), packetId, true));
                OutboundInflightMessage retried = entry.nextQos1Retry(now);
                putOutboundInflight(clientId, packetId, retried, inflight);
            });
            if (inflight.isEmpty()) {
                outboundInflightByClient.remove(clientId, inflight);
            }
        });
    }

    private void retryInflightQos2Messages(SessionRegistry sessionRegistry) {
        long now = System.currentTimeMillis();
        outboundInflightByClient.forEach((clientId, inflight) -> {
            ClientSession session = sessionRegistry == null ? null : sessionRegistry.get(clientId).orElse(null);
            if (session == null || session.channel() == null || !session.channel().isActive()) {
                return;
            }
            Channel channel = session.channel();
            inflight.forEach((packetId, entry) -> {
                if (entry.qos() != MqttQoS.EXACTLY_ONCE.value()) {
                    return;
                }
                if (now - entry.lastSentAtMs() < QOS2_RETRY_INTERVAL_MS) {
                    return;
                }
                if (entry.retryCount() >= QOS2_MAX_RETRIES) {
                    removeOutboundInflight(clientId, packetId, inflight);
                    return;
                }
                if (entry.qos2State() == Qos2State.WAIT_PUBCOMP) {
                    channel.writeAndFlush(MqttPacketFactory.buildPubRelMessage(packetId));
                } else {
                    channel.writeAndFlush(MqttPacketFactory.buildQos2PublishMessage(entry.topic(), entry.payload(), packetId, true));
                }
                OutboundInflightMessage retried = entry.nextQos2Retry(now);
                putOutboundInflight(clientId, packetId, retried, inflight);
            });
            if (inflight.isEmpty()) {
                outboundInflightByClient.remove(clientId, inflight);
            }
        });
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
        ConcurrentMap<Integer, InboundQos2Publish> inbound = new ConcurrentHashMap<>();
        for (Qos2InboundInflightMessage message : qos2InflightStore.listInbound(clientId)) {
            if (message == null || message.packetId() <= 0 || message.topic() == null || message.topic().isBlank()) {
                continue;
            }
            inbound.put(message.packetId(), new InboundQos2Publish(
                    message.topic(),
                    message.payload() == null ? new byte[0] : message.payload().clone(),
                    message.retain()
            ));
        }
        if (inbound.isEmpty()) {
            inboundQos2ByClient.remove(clientId);
            return;
        }
        inboundQos2ByClient.put(clientId, inbound);
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

}
