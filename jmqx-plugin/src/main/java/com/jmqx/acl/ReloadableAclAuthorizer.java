package com.jmqx.acl;

import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ReloadableAclAuthorizer implements AclAuthorizer {
    private volatile AclAuthorizer delegate;

    public ReloadableAclAuthorizer(AclAuthorizer delegate) {
        this.delegate = normalizeDelegate(delegate);
    }

    public void setDelegate(AclAuthorizer delegate) {
        AclAuthorizer previous = this.delegate;
        this.delegate = normalizeDelegate(delegate);
        if (previous != null && previous != this.delegate) {
            previous.close();
        }
    }

    @Override
    public AclDecision authorize(AclRequest request) {
        return delegate.authorize(request);
    }

    @Override
    public CompletableFuture<AclDecision> authorizeAsync(AclRequest request) {
        return delegate.authorizeAsync(request);
    }

    @Override
    public void close() {
        AclAuthorizer current = this.delegate;
        this.delegate = new AllowAllAclAuthorizer();
        if (current != null) {
            current.close();
        }
    }

    private static AclAuthorizer normalizeDelegate(AclAuthorizer delegate) {
        if (delegate == null) {
            return new AllowAllAclAuthorizer();
        }
        return delegate;
    }
}
