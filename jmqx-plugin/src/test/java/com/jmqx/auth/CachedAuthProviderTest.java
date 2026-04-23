package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CachedAuthProviderTest {
    @Test
    void shouldReuseCachedResultForSameClientId() {
        CountingAuthProvider delegate = new CountingAuthProvider();
        CachedAuthProvider provider = new CachedAuthProvider(delegate, 5_000);

        AuthResult first = provider.authenticateResult(new AuthRequest("client-1", "user-a", "password-a"));
        AuthResult second = provider.authenticateResult(new AuthRequest("client-1", "user-b", "password-b"));

        assertEquals(first, second);
        assertEquals(1, delegate.syncCalls.get());
    }

    @Test
    void shouldEvictCachedResultByClientId() {
        CountingAuthProvider delegate = new CountingAuthProvider();
        CachedAuthProvider provider = new CachedAuthProvider(delegate, 5_000);

        provider.authenticateResult(new AuthRequest("client-1", "user-a", "password-a"));
        provider.evictCache("client-1", "user-a");
        provider.authenticateResult(new AuthRequest("client-1", "user-a", "password-a"));

        assertEquals(2, delegate.syncCalls.get());
        assertEquals(1, delegate.evictCalls.get());
    }

    private static class CountingAuthProvider implements AuthProvider {
        private final AtomicInteger syncCalls = new AtomicInteger();
        private final AtomicInteger evictCalls = new AtomicInteger();

        @Override
        public AuthResult authenticateResult(AuthRequest request) {
            int call = syncCalls.incrementAndGet();
            return call == 1 ? AuthResult.allow() : AuthResult.deny();
        }

        @Override
        public void evictCache(String clientId, String username) {
            evictCalls.incrementAndGet();
        }
    }
}
