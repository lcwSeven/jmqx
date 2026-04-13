package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AllowAllAuthProvider implements AuthProvider {
    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        return AuthResult.allow();
    }
}
