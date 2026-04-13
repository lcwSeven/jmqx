package com.jmqx.auth;

import com.jmqx.protocol.AuthDecision;
import com.jmqx.protocol.AuthResult;

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
}
