package com.jmqtt.session;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class InMemorySessionRegistry implements SessionRegistry {
    private final ConcurrentMap<String, ClientSession> sessions = new ConcurrentHashMap<>();

    @Override
    public void register(ClientSession session) {
        ClientSession previous = sessions.put(session.getClientId(), session);
        if (previous != null && previous.getChannel().isActive()) {
            previous.getChannel().close();
        }
    }

    @Override
    public Optional<ClientSession> get(String clientId) {
        return Optional.ofNullable(sessions.get(clientId));
    }

    @Override
    public void remove(String clientId) {
        sessions.remove(clientId);
    }
}
