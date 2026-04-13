package com.jmqx.auth;

import com.jmqx.protocol.AuthDecision;
import com.jmqx.protocol.AuthResult;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AuthProvider {
    default boolean authenticate(AuthRequest request) {
        return authenticateResult(request).decision() == AuthDecision.ALLOW;
    }

    default AuthDecision authenticateDecision(AuthRequest request) {
        return authenticateResult(request).decision();
    }

    AuthResult authenticateResult(AuthRequest request);

    default void close() {
    }
}
