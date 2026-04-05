package com.jmqtt.acl;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AllowAllAclAuthorizer implements AclAuthorizer {
    @Override
    public boolean isAllowed(AclRequest request) {
        return true;
    }
}
