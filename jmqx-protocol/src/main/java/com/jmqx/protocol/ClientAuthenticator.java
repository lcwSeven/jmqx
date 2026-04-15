package com.jmqx.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface ClientAuthenticator {

    AuthResult authenticateResult(String clientId, String username, String password);

    default CompletableFuture<AuthResult> authenticateAsync(String clientId, String username, String password) {
        return CompletableFuture.completedFuture(authenticateResult(clientId, username, password));
    }

    default void evictCache(String clientId, String username) {
    }

}
