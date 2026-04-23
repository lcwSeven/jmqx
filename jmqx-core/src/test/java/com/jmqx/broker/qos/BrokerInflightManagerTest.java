package com.jmqx.broker.qos;

import com.jmqx.router.LocalSubscriptionRegistry;
import com.jmqx.store.qos.Qos1InflightMessage;
import com.jmqx.store.qos.Qos1InflightStore;
import com.jmqx.store.qos.Qos2InboundInflightMessage;
import com.jmqx.store.qos.Qos2InflightStore;
import com.jmqx.store.qos.Qos2OutboundInflightMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerInflightManagerTest {
    @Test
    void shouldAdvanceQoS2OutboundStateOnPubRecAndCleanupOnPubComp() {
        InMemoryQos1InflightStore qos1Store = new InMemoryQos1InflightStore();
        InMemoryQos2InflightStore qos2Store = new InMemoryQos2InflightStore();
        BrokerInflightManager manager = new BrokerInflightManager(qos1Store, qos2Store, Logger.getAnonymousLogger());

        manager.trackInflightQos2("client-1", 10, "sensor/temp", new byte[]{1, 2, 3});

        assertTrue(manager.onPubRec("client-1", 10));
        Qos2OutboundInflightMessage persisted = qos2Store.getOutbound("client-1", 10).orElseThrow();
        assertEquals(Qos2State.WAIT_PUBCOMP.code(), persisted.state());

        manager.onPubComp("client-1", 10);

        assertTrue(qos2Store.listOutbound("client-1").isEmpty());
    }

    @Test
    void shouldProcessCompleteAndRemoveInboundQoS2Flow() {
        InMemoryQos1InflightStore qos1Store = new InMemoryQos1InflightStore();
        InMemoryQos2InflightStore qos2Store = new InMemoryQos2InflightStore();
        BrokerInflightManager manager = new BrokerInflightManager(qos1Store, qos2Store, Logger.getAnonymousLogger());

        manager.saveInboundQos2("client-1", 20, "sensor/temp", new byte[]{9, 8}, true);

        BrokerInflightManager.InboundQos2ReleaseDecision processDecision = manager.onPubRel("client-1", 20);
        assertTrue(processDecision.shouldProcess());
        assertTrue(processDecision.shouldCleanupAfterAck());
        assertEquals("sensor/temp", processDecision.publish().topic());
        assertArrayEquals(new byte[]{9, 8}, processDecision.publish().payload());
        assertTrue(processDecision.publish().retain());

        manager.markInboundQos2Completed("client-1", 20);
        BrokerInflightManager.InboundQos2ReleaseDecision completedDecision = manager.onPubRel("client-1", 20);
        assertFalse(completedDecision.shouldProcess());
        assertTrue(completedDecision.shouldCleanupAfterAck());

        manager.removeInboundQos2("client-1", 20);
        BrokerInflightManager.InboundQos2ReleaseDecision missingDecision = manager.onPubRel("client-1", 20);
        assertFalse(missingDecision.shouldProcess());
        assertFalse(missingDecision.shouldCleanupAfterAck());
    }

    @Test
    void shouldReportPersistedSessionStateFromSubscriptionsAndInflightStores() {
        InMemoryQos1InflightStore qos1Store = new InMemoryQos1InflightStore();
        InMemoryQos2InflightStore qos2Store = new InMemoryQos2InflightStore();
        BrokerInflightManager manager = new BrokerInflightManager(qos1Store, qos2Store, Logger.getAnonymousLogger());
        LocalSubscriptionRegistry registry = new LocalSubscriptionRegistry();

        assertFalse(manager.hasPersistedSessionState("client-1", registry));

        registry.subscribeAndCheckFirst("client-1", "sensor/temp", 1);
        assertTrue(manager.hasPersistedSessionState("client-1", registry));

        registry.removeClientAndCollectLastTopics("client-1");
        assertFalse(manager.hasPersistedSessionState("client-1", registry));

        qos1Store.save("client-1", new Qos1InflightMessage(1, "sensor/temp", new byte[]{1}, System.currentTimeMillis(), 0));
        assertTrue(manager.hasPersistedSessionState("client-1", registry));

        qos1Store.removeClient("client-1");
        assertFalse(manager.hasPersistedSessionState("client-1", registry));

        qos2Store.saveOutbound("client-1", new Qos2OutboundInflightMessage(2, "sensor/temp", new byte[]{2}, Qos2State.WAIT_PUBREC.code(), System.currentTimeMillis()));
        assertTrue(manager.hasPersistedSessionState("client-1", registry));

        qos2Store.removeOutboundClient("client-1");
        assertFalse(manager.hasPersistedSessionState("client-1", registry));

        qos2Store.saveInbound("client-1", Qos2InboundInflightMessage.waitingPubRel(3, "sensor/temp", new byte[]{3}, false));
        assertTrue(manager.hasPersistedSessionState("client-1", registry));
    }

    private static class InMemoryQos1InflightStore implements Qos1InflightStore {
        private final ConcurrentMap<String, ConcurrentMap<Integer, Qos1InflightMessage>> data = new ConcurrentHashMap<>();

        @Override
        public void save(String clientId, Qos1InflightMessage message) {
            data.computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>()).put(message.packetId(), message);
        }

        @Override
        public void remove(String clientId, int packetId) {
            ConcurrentMap<Integer, Qos1InflightMessage> messages = data.get(clientId);
            if (messages != null) {
                messages.remove(packetId);
                if (messages.isEmpty()) {
                    data.remove(clientId, messages);
                }
            }
        }

        @Override
        public void removeClient(String clientId) {
            data.remove(clientId);
        }

        @Override
        public List<Qos1InflightMessage> listByClient(String clientId) {
            ConcurrentMap<Integer, Qos1InflightMessage> messages = data.get(clientId);
            return messages == null ? List.of() : new ArrayList<>(messages.values());
        }

        @Override
        public int maxPacketId(String clientId) {
            ConcurrentMap<Integer, Qos1InflightMessage> messages = data.get(clientId);
            if (messages == null || messages.isEmpty()) {
                return 0;
            }
            return messages.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
        }
    }

    private static class InMemoryQos2InflightStore implements Qos2InflightStore {
        private final ConcurrentMap<String, ConcurrentMap<Integer, Qos2OutboundInflightMessage>> outbound = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, ConcurrentMap<Integer, Qos2InboundInflightMessage>> inbound = new ConcurrentHashMap<>();

        @Override
        public void saveOutbound(String clientId, Qos2OutboundInflightMessage message) {
            outbound.computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>()).put(message.packetId(), message);
        }

        @Override
        public void removeOutbound(String clientId, int packetId) {
            ConcurrentMap<Integer, Qos2OutboundInflightMessage> messages = outbound.get(clientId);
            if (messages != null) {
                messages.remove(packetId);
                if (messages.isEmpty()) {
                    outbound.remove(clientId, messages);
                }
            }
        }

        @Override
        public void removeOutboundClient(String clientId) {
            outbound.remove(clientId);
        }

        @Override
        public List<Qos2OutboundInflightMessage> listOutbound(String clientId) {
            ConcurrentMap<Integer, Qos2OutboundInflightMessage> messages = outbound.get(clientId);
            return messages == null ? List.of() : new ArrayList<>(messages.values());
        }

        @Override
        public int maxOutboundPacketId(String clientId) {
            ConcurrentMap<Integer, Qos2OutboundInflightMessage> messages = outbound.get(clientId);
            if (messages == null || messages.isEmpty()) {
                return 0;
            }
            return messages.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
        }

        @Override
        public void saveInbound(String clientId, Qos2InboundInflightMessage message) {
            inbound.computeIfAbsent(clientId, ignored -> new ConcurrentHashMap<>()).put(message.packetId(), message);
        }

        @Override
        public void removeInbound(String clientId, int packetId) {
            ConcurrentMap<Integer, Qos2InboundInflightMessage> messages = inbound.get(clientId);
            if (messages != null) {
                messages.remove(packetId);
                if (messages.isEmpty()) {
                    inbound.remove(clientId, messages);
                }
            }
        }

        @Override
        public void removeInboundClient(String clientId) {
            inbound.remove(clientId);
        }

        @Override
        public Optional<Qos2InboundInflightMessage> getInbound(String clientId, int packetId) {
            ConcurrentMap<Integer, Qos2InboundInflightMessage> messages = inbound.get(clientId);
            if (messages == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(messages.get(packetId));
        }

        @Override
        public List<Qos2InboundInflightMessage> listInbound(String clientId) {
            ConcurrentMap<Integer, Qos2InboundInflightMessage> messages = inbound.get(clientId);
            return messages == null ? List.of() : new ArrayList<>(messages.values());
        }

        private Optional<Qos2OutboundInflightMessage> getOutbound(String clientId, int packetId) {
            ConcurrentMap<Integer, Qos2OutboundInflightMessage> messages = outbound.get(clientId);
            if (messages == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(messages.get(packetId));
        }
    }
}
