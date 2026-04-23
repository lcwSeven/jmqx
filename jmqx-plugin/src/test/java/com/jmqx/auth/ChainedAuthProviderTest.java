package com.jmqx.auth;

import com.jmqx.protocol.AuthDecision;
import com.jmqx.protocol.AuthResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainedAuthProviderTest {
    @Test
    void shouldContinueUntilProviderAllows() {
        CountingAuthProvider first = new CountingAuthProvider(AuthResult.notFound());
        CountingAuthProvider second = new CountingAuthProvider(AuthResult.allow(true));
        CountingAuthProvider third = new CountingAuthProvider(AuthResult.deny());
        ChainedAuthProvider provider = new ChainedAuthProvider(List.of(first, second, third));

        AuthResult result = provider.authenticateResult(new AuthRequest("client-1", "u", "p"));

        assertEquals(AuthDecision.ALLOW, result.decision());
        assertTrue(result.superuser());
        assertEquals(1, first.syncCalls.get());
        assertEquals(1, second.syncCalls.get());
        assertEquals(0, third.syncCalls.get());
    }

    @Test
    void shouldStopWhenProviderDenies() {
        CountingAuthProvider first = new CountingAuthProvider(AuthResult.notFound());
        CountingAuthProvider second = new CountingAuthProvider(AuthResult.deny());
        CountingAuthProvider third = new CountingAuthProvider(AuthResult.allow());
        ChainedAuthProvider provider = new ChainedAuthProvider(List.of(first, second, third));

        AuthResult result = provider.authenticateResult(new AuthRequest("client-1", "u", "p"));

        assertEquals(AuthDecision.DENY, result.decision());
        assertEquals(1, first.syncCalls.get());
        assertEquals(1, second.syncCalls.get());
        assertEquals(0, third.syncCalls.get());
    }

    @Test
    void shouldSupportAsyncTraversal() {
        CountingAuthProvider first = new CountingAuthProvider(AuthResult.notFound());
        CountingAuthProvider second = new CountingAuthProvider(AuthResult.allow());
        ChainedAuthProvider provider = new ChainedAuthProvider(List.of(first, second));

        AuthResult result = provider.authenticateAsync(new AuthRequest("client-1", "u", "p")).join();

        assertEquals(AuthDecision.ALLOW, result.decision());
        assertEquals(1, first.asyncCalls.get());
        assertEquals(1, second.asyncCalls.get());
    }

    private static class CountingAuthProvider implements AuthProvider {
        private final AuthResult result;
        private final AtomicInteger syncCalls = new AtomicInteger();
        private final AtomicInteger asyncCalls = new AtomicInteger();

        private CountingAuthProvider(AuthResult result) {
            this.result = result;
        }

        @Override
        public AuthResult authenticateResult(AuthRequest request) {
            syncCalls.incrementAndGet();
            return result;
        }

        @Override
        public CompletableFuture<AuthResult> authenticateAsync(AuthRequest request) {
            asyncCalls.incrementAndGet();
            return CompletableFuture.completedFuture(result);
        }
    }
}
