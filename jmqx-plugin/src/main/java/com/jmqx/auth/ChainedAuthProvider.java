package com.jmqx.auth;

import com.jmqx.protocol.AuthDecision;
import com.jmqx.protocol.AuthResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class ChainedAuthProvider implements AuthProvider {
    private final List<AuthProvider> chain;

    public ChainedAuthProvider(List<AuthProvider> chain) {
        this.chain = chain;
    }

    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        if (chain == null || chain.isEmpty()) {
            return AuthResult.deny();
        }
        for (AuthProvider provider : chain) {
            if (provider == null) {
                continue;
            }
            AuthResult result = provider.authenticateResult(request);
            if (result.decision() == AuthDecision.ALLOW) {
                return result;
            }
            if (result.decision() == AuthDecision.DENY) {
                return AuthResult.deny();
            }
        }
        return AuthResult.deny();
    }

    @Override
    public CompletableFuture<AuthResult> authenticateAsync(AuthRequest request) {
        return authenticateAsync(request, 0);
    }

    @Override
    public void evictCache(String clientId, String username) {
        if (chain == null || chain.isEmpty()) {
            return;
        }
        for (AuthProvider provider : chain) {
            if (provider != null) {
                provider.evictCache(clientId, username);
            }
        }
    }

    @Override
    public void close() {
        if (chain == null || chain.isEmpty()) {
            return;
        }
        for (AuthProvider provider : chain) {
            if (provider != null) {
                provider.close();
            }
        }
    }

    private CompletableFuture<AuthResult> authenticateAsync(AuthRequest request, int index) {
        if (chain == null || index >= chain.size()) {
            return CompletableFuture.completedFuture(AuthResult.deny());
        }
        AuthProvider provider = chain.get(index);
        if (provider == null) {
            return authenticateAsync(request, index + 1);
        }
        return provider.authenticateAsync(request).thenCompose(result -> {
            if (result == null) {
                return authenticateAsync(request, index + 1);
            }
            if (result.decision() == AuthDecision.ALLOW || result.decision() == AuthDecision.DENY) {
                return CompletableFuture.completedFuture(result);
            }
            return authenticateAsync(request, index + 1);
        });
    }
}
