package com.jmqx.acl;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/14
 */
public class ChainedAclAuthorizer implements AclAuthorizer {
    private final List<AclAuthorizer> chain;
    private final boolean defaultAllow;

    public ChainedAclAuthorizer(List<AclAuthorizer> chain, boolean defaultAllow) {
        this.chain = chain;
        this.defaultAllow = defaultAllow;
    }

    @Override
    public AclDecision authorize(AclRequest request) {
        if (chain != null) {
            for (AclAuthorizer authorizer : chain) {
                if (authorizer == null) {
                    continue;
                }
                AclDecision decision = authorizer.authorize(request);
                if (decision == AclDecision.ALLOW || decision == AclDecision.DENY) {
                    return decision;
                }
            }
        }
        return defaultAllow ? AclDecision.ALLOW : AclDecision.DENY;
    }

    @Override
    public CompletableFuture<AclDecision> authorizeAsync(AclRequest request) {
        return authorizeAsync(request, 0);
    }

    private CompletableFuture<AclDecision> authorizeAsync(AclRequest request, int index) {
        if (chain == null || index >= chain.size()) {
            return CompletableFuture.completedFuture(defaultAllow ? AclDecision.ALLOW : AclDecision.DENY);
        }
        AclAuthorizer authorizer = chain.get(index);
        if (authorizer == null) {
            return authorizeAsync(request, index + 1);
        }
        return authorizer.authorizeAsync(request).thenCompose(decision -> {
            if (decision == AclDecision.ALLOW || decision == AclDecision.DENY) {
                return CompletableFuture.completedFuture(decision);
            }
            return authorizeAsync(request, index + 1);
        });
    }
}
