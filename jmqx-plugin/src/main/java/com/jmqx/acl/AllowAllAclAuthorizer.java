package com.jmqx.acl;

import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AllowAllAclAuthorizer implements AclAuthorizer {
    @Override
    public AclDecision authorize(AclRequest request) {
        return AclDecision.ALLOW;
    }

    @Override
    public CompletableFuture<AclDecision> authorizeAsync(AclRequest request) {
        return CompletableFuture.completedFuture(AclDecision.ALLOW);
    }
}
