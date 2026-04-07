package com.jmqx.auth;

import java.util.List;

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
    public boolean authenticate(AuthRequest request) {
        return authenticateDecision(request) == AuthDecision.ALLOW;
    }

    @Override
    public AuthDecision authenticateDecision(AuthRequest request) {
        if (chain == null || chain.isEmpty()) {
            return AuthDecision.DENY;
        }
        for (AuthProvider provider : chain) {
            if (provider == null) {
                continue;
            }
            AuthDecision decision = provider.authenticateDecision(request);
            if (decision == AuthDecision.ALLOW) {
                return AuthDecision.ALLOW;
            }
            if (decision == AuthDecision.DENY) {
                return AuthDecision.DENY;
            }
        }
        return AuthDecision.DENY;
    }
}
