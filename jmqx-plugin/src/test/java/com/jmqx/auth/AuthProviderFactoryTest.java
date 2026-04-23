package com.jmqx.auth;

import com.jmqx.protocol.AuthDecision;
import com.jmqx.protocol.AuthResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class AuthProviderFactoryTest {
    @Test
    void shouldFallbackToAllowAllWhenChainIsEmpty() {
        AuthProperties properties = new AuthProperties();
        properties.setChain("");
        properties.setCacheMillis(0);

        AuthProvider provider = AuthProviderFactory.create(properties);
        AuthResult result = provider.authenticateResult(new AuthRequest("client-1", "user", "password"));

        assertEquals(AuthDecision.ALLOW, result.decision());
    }

    @Test
    void shouldFallbackToAllowAllWhenChainContainsOnlyUnknownProviders() {
        AuthProperties properties = new AuthProperties();
        properties.setChain("unknown-provider");
        properties.setCacheMillis(0);

        AuthProvider provider = AuthProviderFactory.create(properties);
        AuthResult result = provider.authenticateResult(new AuthRequest("client-1", "user", "password"));

        assertEquals(AuthDecision.ALLOW, result.decision());
    }

    @Test
    void shouldWrapDelegateWithCacheWhenEnabled() {
        AuthProperties properties = new AuthProperties();
        properties.setChain("");
        properties.setCacheMillis(1_000);

        AuthProvider provider = AuthProviderFactory.create(properties);

        assertInstanceOf(CachedAuthProvider.class, provider);
    }
}
