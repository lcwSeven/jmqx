package com.jmqx.acl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class AclAuthorizerFactoryTest {
    @Test
    void shouldFallbackToAllowAllWhenChainIsEmpty() {
        AclProperties properties = new AclProperties();
        properties.setChain("");
        properties.setCacheMillis(0);

        AclAuthorizer authorizer = AclAuthorizerFactory.create(properties);
        AclDecision decision = authorizer.authorize(new AclRequest("client-1", "user", "sensor/temp", AclAction.SUBSCRIBE));

        assertEquals(AclDecision.ALLOW, decision);
    }

    @Test
    void shouldUseDefaultDecisionWhenChainContainsOnlyUnknownProviders() {
        AclProperties properties = new AclProperties();
        properties.setChain("unknown-provider");
        properties.setDefaultAllow(false);
        properties.setCacheMillis(0);

        AclAuthorizer authorizer = AclAuthorizerFactory.create(properties);
        AclDecision decision = authorizer.authorize(new AclRequest("client-1", "user", "sensor/temp", AclAction.SUBSCRIBE));

        assertEquals(AclDecision.DENY, decision);
    }

    @Test
    void shouldWrapDelegateWithCacheWhenEnabled() {
        AclProperties properties = new AclProperties();
        properties.setChain("");
        properties.setCacheMillis(1_000);

        AclAuthorizer authorizer = AclAuthorizerFactory.create(properties);

        assertInstanceOf(CachedAclAuthorizer.class, authorizer);
    }
}
