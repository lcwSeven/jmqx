package com.jmqx.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 本地节点会话注册表（内存实现）。
 *
 * @author liucaiwen
 * @date 2026/4/11
 */
public class LocalSessionRegistry implements SessionRegistry {
    // 本地会话
    private final ConcurrentMap<String, ClientSession> sessions = new ConcurrentHashMap<>();

    @Override
    public void register(ClientSession session) {
        // 这里需要确保会话对象是唯一的，因此需要检查当前会话对象是否已经存在。
        ClientSession previous = sessions.put(session.clientId(), session);
        if (previous != null && previous.channel().isActive()) {
            previous.channel().close();
        }
    }

    @Override
    public Optional<ClientSession> get(String clientId) {
        return Optional.ofNullable(sessions.get(clientId));
    }

    @Override
    public List<ClientSession> list() {
        return new ArrayList<>(sessions.values());
    }

    @Override
    public void remove(String clientId) {
        sessions.remove(clientId);
    }
}
