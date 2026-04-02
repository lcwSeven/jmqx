/**
 * @author liucaiwen
 * @date 2026/4/2
 */
package com.jmqtt.session;

import java.util.Optional;

public interface SessionRegistry {
    void register(ClientSession session);

    Optional<ClientSession> get(String clientId);

    void remove(String clientId);
}
