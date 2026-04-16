package com.jmqx.acl;

import java.util.ArrayList;
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
        this.chain = chain == null ? new ArrayList<>() : new ArrayList<>(chain);
        this.defaultAllow = defaultAllow;
    }

    @Override
    public AclDecision authorize(AclRequest request) {
        for (AclAuthorizer authorizer : chain) {
            AclDecision decision = authorizeBySingleAuthorizer(authorizer, request);
            if (decision == AclDecision.ALLOW || decision == AclDecision.DENY) {
                return decision;
            }
        }
        return defaultAllow ? AclDecision.ALLOW : AclDecision.DENY;
    }

    @Override
    public CompletableFuture<AclDecision> authorizeAsync(AclRequest request) {
        CompletableFuture<AclDecision> future = new CompletableFuture<>();
        authorizeAsync(request, 0, future);
        return future;
    }

    private AclDecision authorizeBySingleAuthorizer(AclAuthorizer authorizer, AclRequest request) {
        if (authorizer == null) {
            return AclDecision.NOT_FOUND;
        }
        return authorizer.authorize(request);
    }

    private void authorizeAsync(AclRequest request, int index, CompletableFuture<AclDecision> future) {
        if (index >= chain.size()) {
            future.complete(defaultAllow ? AclDecision.ALLOW : AclDecision.DENY);
            return;
        }
        AclAuthorizer authorizer = chain.get(index);
        if (authorizer == null) {
            authorizeAsync(request, index + 1, future);
            return;
        }
        authorizer.authorizeAsync(request).whenComplete((decision, error) -> {
            if (error != null) {
                future.completeExceptionally(error);
                return;
            }
            if (decision == AclDecision.ALLOW || decision == AclDecision.DENY) {
                future.complete(decision);
                return;
            }
            authorizeAsync(request, index + 1, future);
        });
    }
}
