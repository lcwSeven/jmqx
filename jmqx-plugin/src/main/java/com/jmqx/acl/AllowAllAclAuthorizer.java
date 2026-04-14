package com.jmqx.acl;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AllowAllAclAuthorizer implements AclAuthorizer {
    @Override
    public AclDecision authorize(AclRequest request) {
        return AclDecision.ALLOW;
    }
}
