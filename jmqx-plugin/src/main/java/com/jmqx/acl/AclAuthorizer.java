package com.jmqx.acl;

import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AclAuthorizer {
    AclDecision authorize(AclRequest request);

    default CompletableFuture<AclDecision> authorizeAsync(AclRequest request) {
        return CompletableFuture.completedFuture(authorize(request));
    }

    default boolean isAllowed(AclRequest request) {
        return authorize(request) == AclDecision.ALLOW;
    }

    default void close() {
    }
}
