package com.jmqx.auth;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ReloadableAuthProvider implements AuthProvider {
    private volatile AuthProvider delegate;

    public ReloadableAuthProvider(AuthProvider delegate) {
        this.delegate = delegate;
    }

    public void setDelegate(AuthProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean authenticate(AuthRequest request) {
        return delegate.authenticate(request);
    }
}
