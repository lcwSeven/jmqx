package com.jmqx.store;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * QoS2 inflight 存储接口（包含上行与下行状态）。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public interface Qos2InflightStore extends AutoCloseable {
    Qos2InflightStore NOOP = new Qos2InflightStore() {
        @Override
        public void saveOutbound(String clientId, Qos2OutboundInflightMessage message) {
        }

        @Override
        public void removeOutbound(String clientId, int packetId) {
        }

        @Override
        public void removeOutboundClient(String clientId) {
        }

        @Override
        public List<Qos2OutboundInflightMessage> listOutbound(String clientId) {
            return Collections.emptyList();
        }

        @Override
        public int maxOutboundPacketId(String clientId) {
            return 0;
        }

        @Override
        public void saveInbound(String clientId, Qos2InboundInflightMessage message) {
        }

        @Override
        public void removeInbound(String clientId, int packetId) {
        }

        @Override
        public void removeInboundClient(String clientId) {
        }

        @Override
        public Optional<Qos2InboundInflightMessage> getInbound(String clientId, int packetId) {
            return Optional.empty();
        }

        @Override
        public List<Qos2InboundInflightMessage> listInbound(String clientId) {
            return Collections.emptyList();
        }
    };

    void saveOutbound(String clientId, Qos2OutboundInflightMessage message);

    void removeOutbound(String clientId, int packetId);

    void removeOutboundClient(String clientId);

    List<Qos2OutboundInflightMessage> listOutbound(String clientId);

    int maxOutboundPacketId(String clientId);

    void saveInbound(String clientId, Qos2InboundInflightMessage message);

    void removeInbound(String clientId, int packetId);

    void removeInboundClient(String clientId);

    Optional<Qos2InboundInflightMessage> getInbound(String clientId, int packetId);

    List<Qos2InboundInflightMessage> listInbound(String clientId);

    @Override
    default void close() {
    }
}
