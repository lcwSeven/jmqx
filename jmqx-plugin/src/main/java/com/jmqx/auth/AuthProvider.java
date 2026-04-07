package com.jmqx.auth;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AuthProvider {
    boolean authenticate(AuthRequest request);

    default AuthDecision authenticateDecision(AuthRequest request) {
        return authenticate(request) ? AuthDecision.ALLOW : AuthDecision.DENY;
    }
}
