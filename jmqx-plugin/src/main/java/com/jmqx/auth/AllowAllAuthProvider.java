package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;

import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AllowAllAuthProvider implements AuthProvider {
    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        return AuthResult.allow();
    }

    @Override
    public CompletableFuture<AuthResult> authenticateAsync(AuthRequest request) {
        return CompletableFuture.completedFuture(AuthResult.allow());
    }
}
