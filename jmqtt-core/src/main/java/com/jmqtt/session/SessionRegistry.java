package com.jmqtt.session;

import java.util.List;
import java.util.Optional;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface SessionRegistry {
    void register(ClientSession session);

    Optional<ClientSession> get(String clientId);

    List<ClientSession> list();

    void remove(String clientId);
}
