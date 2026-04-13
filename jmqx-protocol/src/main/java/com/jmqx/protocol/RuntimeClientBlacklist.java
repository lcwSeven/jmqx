package com.jmqx.protocol;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 运行时黑名单实现。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public class RuntimeClientBlacklist implements ClientBlacklist {

    private final Set<String> blockedClientIds = ConcurrentHashMap.newKeySet();
    private final Set<String> blockedIps = ConcurrentHashMap.newKeySet();

    @Override
    public boolean isBlocked(String clientId, String clientIp) {
        return blockedClientIds.contains(normalize(clientId)) || blockedIps.contains(normalize(clientIp));
    }

    @Override
    public void upsert(String type, String value) {
        String normalizedType = normalizeType(type);
        String normalizedValue = normalize(value);
        if (normalizedValue.isBlank()) {
            return;
        }
        if (TYPE_IP.equals(normalizedType)) {
            blockedIps.add(normalizedValue);
            return;
        }
        blockedClientIds.add(normalizedValue);
    }

    @Override
    public void remove(String type, String value) {
        String normalizedType = normalizeType(type);
        String normalizedValue = normalize(value);
        if (normalizedValue.isBlank()) {
            return;
        }
        if (TYPE_IP.equals(normalizedType)) {
            blockedIps.remove(normalizedValue);
            return;
        }
        blockedClientIds.remove(normalizedValue);
    }

    private static String normalizeType(String type) {
        return TYPE_IP.equalsIgnoreCase(normalize(type)) ? TYPE_IP : TYPE_CLIENT_ID;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }
}
