package com.jmqx.common.logging;

import org.slf4j.MDC;

/**
 * 客户端日志上下文。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public final class ClientLogContext {

    public static final String CLIENT_ID_KEY = "clientId";

    private ClientLogContext() {
    }

    public static Scope open(String clientId) {
        String previous = MDC.get(CLIENT_ID_KEY);
        if (clientId == null || clientId.isBlank()) {
            MDC.remove(CLIENT_ID_KEY);
        } else {
            MDC.put(CLIENT_ID_KEY, clientId.trim());
        }
        return () -> {
            if (previous == null || previous.isBlank()) {
                MDC.remove(CLIENT_ID_KEY);
            } else {
                MDC.put(CLIENT_ID_KEY, previous);
            }
        };
    }

    @FunctionalInterface
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
