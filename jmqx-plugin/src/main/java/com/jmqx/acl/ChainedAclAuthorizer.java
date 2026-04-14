package com.jmqx.acl;

import java.util.List;

/**
 * @author liucaiwen
 * @date 2026/4/14
 */
public class ChainedAclAuthorizer implements AclAuthorizer {
    private final List<AclAuthorizer> chain;
    private final boolean defaultAllow;

    public ChainedAclAuthorizer(List<AclAuthorizer> chain, boolean defaultAllow) {
        this.chain = chain;
        this.defaultAllow = defaultAllow;
    }

    @Override
    public AclDecision authorize(AclRequest request) {
        if (chain != null) {
            for (AclAuthorizer authorizer : chain) {
                if (authorizer == null) {
                    continue;
                }
                AclDecision decision = authorizer.authorize(request);
                if (decision == AclDecision.ALLOW || decision == AclDecision.DENY) {
                    return decision;
                }
            }
        }
        return defaultAllow ? AclDecision.ALLOW : AclDecision.DENY;
    }
}
