package com.jmqx.acl;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChainedAclAuthorizerTest {
    @Test
    void shouldStopAtFirstConcreteDecision() {
        CountingAclAuthorizer first = new CountingAclAuthorizer(AclDecision.NOT_FOUND);
        CountingAclAuthorizer second = new CountingAclAuthorizer(AclDecision.DENY);
        CountingAclAuthorizer third = new CountingAclAuthorizer(AclDecision.ALLOW);
        ChainedAclAuthorizer authorizer = new ChainedAclAuthorizer(List.of(first, second, third), true);

        AclDecision decision = authorizer.authorize(new AclRequest("client-1", "user", "sensor/temp", AclAction.SUBSCRIBE));

        assertEquals(AclDecision.DENY, decision);
        assertEquals(1, first.syncCalls.get());
        assertEquals(1, second.syncCalls.get());
        assertEquals(0, third.syncCalls.get());
    }

    @Test
    void shouldFallbackToDefaultAllowWhenNothingMatches() {
        ChainedAclAuthorizer authorizer = new ChainedAclAuthorizer(
            List.of(new CountingAclAuthorizer(AclDecision.NOT_FOUND)),
            true
        );

        AclDecision decision = authorizer.authorize(new AclRequest("client-1", "user", "sensor/temp", AclAction.PUBLISH));

        assertEquals(AclDecision.ALLOW, decision);
    }

    @Test
    void shouldSupportAsyncTraversal() {
        CountingAclAuthorizer first = new CountingAclAuthorizer(AclDecision.NOT_FOUND);
        CountingAclAuthorizer second = new CountingAclAuthorizer(AclDecision.ALLOW);
        ChainedAclAuthorizer authorizer = new ChainedAclAuthorizer(List.of(first, second), false);

        AclDecision decision = authorizer.authorizeAsync(
            new AclRequest("client-1", "user", "sensor/temp", AclAction.SUBSCRIBE)
        ).join();

        assertEquals(AclDecision.ALLOW, decision);
        assertEquals(1, first.asyncCalls.get());
        assertEquals(1, second.asyncCalls.get());
    }

    private static class CountingAclAuthorizer implements AclAuthorizer {
        private final AclDecision decision;
        private final AtomicInteger syncCalls = new AtomicInteger();
        private final AtomicInteger asyncCalls = new AtomicInteger();

        private CountingAclAuthorizer(AclDecision decision) {
            this.decision = decision;
        }

        @Override
        public AclDecision authorize(AclRequest request) {
            syncCalls.incrementAndGet();
            return decision;
        }

        @Override
        public CompletableFuture<AclDecision> authorizeAsync(AclRequest request) {
            asyncCalls.incrementAndGet();
            return CompletableFuture.completedFuture(decision);
        }
    }
}
