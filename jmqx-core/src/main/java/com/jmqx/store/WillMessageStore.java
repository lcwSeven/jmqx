package com.jmqx.store;

import com.jmqx.broker.core.WillMessage;

import java.util.Optional;

/**
 * 遗嘱消息持久化存储接口。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public interface WillMessageStore extends AutoCloseable {
    WillMessageStore NOOP = new WillMessageStore() {
        @Override
        public void save(String clientId, WillMessage willMessage) {
        }

        @Override
        public Optional<WillMessage> get(String clientId) {
            return Optional.empty();
        }

        @Override
        public void remove(String clientId) {
        }
    };

    void save(String clientId, WillMessage willMessage);

    Optional<WillMessage> get(String clientId);

    void remove(String clientId);

    @Override
    default void close() {
    }
}

