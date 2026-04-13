package com.jmqx.protocol;

/**
 * 客户端黑名单。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public interface ClientBlacklist {

    String TYPE_CLIENT_ID = "clientId";
    String TYPE_IP = "ip";

    ClientBlacklist NOOP = new ClientBlacklist() {
    };

    default boolean isBlocked(String clientId, String clientIp) {
        return false;
    }

    default void upsert(String type, String value) {
    }

    default void remove(String type, String value) {
    }
}
