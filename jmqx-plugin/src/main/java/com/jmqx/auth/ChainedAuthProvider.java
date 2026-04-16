package com.jmqx.auth;

import com.jmqx.protocol.AuthDecision;
import com.jmqx.protocol.AuthResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class ChainedAuthProvider implements AuthProvider {
    private final List<AuthProvider> chain;

    public ChainedAuthProvider(List<AuthProvider> chain) {
        this.chain = chain == null ? new ArrayList<>() : new ArrayList<>(chain);
    }

    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        if (chain.isEmpty()) {
            return AuthResult.deny();
        }
        for (AuthProvider provider : chain) {
            AuthResult result = authenticateBySingleProvider(provider, request);
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
        CompletableFuture<AuthResult> future = new CompletableFuture<>();
        authenticateAsync(request, 0, future);
        return future;
    }

    @Override
    public void evictCache(String clientId, String username) {
        for (AuthProvider provider : chain) {
            if (provider != null) {
                provider.evictCache(clientId, username);
            }
        }
    }

    @Override
    public void close() {
        for (AuthProvider provider : chain) {
            if (provider != null) {
                provider.close();
            }
        }
    }

    private AuthResult authenticateBySingleProvider(AuthProvider provider, AuthRequest request) {
        if (provider == null) {
            return AuthResult.notFound();
        }
        return provider.authenticateResult(request);
    }

    private void authenticateAsync(AuthRequest request, int index, CompletableFuture<AuthResult> future) {
        if (index >= chain.size()) {
            future.complete(AuthResult.deny());
            return;
        }
        AuthProvider provider = chain.get(index);
        if (provider == null) {
            authenticateAsync(request, index + 1, future);
            return;
        }
        provider.authenticateAsync(request).whenComplete((result, error) -> {
            if (error != null) {
                future.completeExceptionally(error);
                return;
            }
            if (result == null) {
                authenticateAsync(request, index + 1, future);
                return;
            }
            if (result.decision() == AuthDecision.ALLOW || result.decision() == AuthDecision.DENY) {
                future.complete(result);
                return;
            }
            authenticateAsync(request, index + 1, future);
        });
    }
}
