package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;

import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ReloadableAuthProvider implements AuthProvider {
    private volatile AuthProvider delegate;

    public ReloadableAuthProvider(AuthProvider delegate) {
        this.delegate = normalizeDelegate(delegate);
    }

    public void setDelegate(AuthProvider delegate) {
        AuthProvider previous = this.delegate;
        this.delegate = normalizeDelegate(delegate);
        if (previous != null && previous != this.delegate) {
            previous.close();
        }
    }

    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        return delegate.authenticateResult(request);
    }

    @Override
    public CompletableFuture<AuthResult> authenticateAsync(AuthRequest request) {
        return delegate.authenticateAsync(request);
    }

    @Override
    public void evictCache(String clientId, String username) {
        delegate.evictCache(clientId, username);
    }

    @Override
    public void close() {
        AuthProvider current = this.delegate;
        this.delegate = new AllowAllAuthProvider();
        if (current != null) {
            current.close();
        }
    }

    private static AuthProvider normalizeDelegate(AuthProvider delegate) {
        if (delegate == null) {
            return new AllowAllAuthProvider();
        }
        return delegate;
    }
}
