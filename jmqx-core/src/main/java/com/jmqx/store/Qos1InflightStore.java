package com.jmqx.store;

import java.util.Collections;
import java.util.List;

/**
 * QoS1 下行 inflight 存储接口。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public interface Qos1InflightStore extends AutoCloseable {
    Qos1InflightStore NOOP = new Qos1InflightStore() {
        @Override
        public void save(String clientId, Qos1InflightMessage message) {
        }

        @Override
        public void remove(String clientId, int packetId) {
        }

        @Override
        public void removeClient(String clientId) {
        }

        @Override
        public List<Qos1InflightMessage> listByClient(String clientId) {
            return Collections.emptyList();
        }

        @Override
        public int maxPacketId(String clientId) {
            return 0;
        }
    };

    void save(String clientId, Qos1InflightMessage message);

    void remove(String clientId, int packetId);

    void removeClient(String clientId);

    List<Qos1InflightMessage> listByClient(String clientId);

    int maxPacketId(String clientId);

    @Override
    default void close() {
    }
}
