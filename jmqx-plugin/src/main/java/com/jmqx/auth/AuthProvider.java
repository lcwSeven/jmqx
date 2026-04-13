package com.jmqx.auth;

import com.jmqx.protocol.AuthDecision;
import com.jmqx.protocol.AuthResult;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AuthProvider {



    AuthResult authenticateResult(AuthRequest request);

    default void evictCache(String clientId, String username) {
    }

    default void close() {
    }
}
