package com.jmqx.acl;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ReloadableAclAuthorizer implements AclAuthorizer {
    private volatile AclAuthorizer delegate;

    public ReloadableAclAuthorizer(AclAuthorizer delegate) {
        this.delegate = delegate;
    }

    public void setDelegate(AclAuthorizer delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isAllowed(AclRequest request) {
        return delegate.isAllowed(request);
    }
}
