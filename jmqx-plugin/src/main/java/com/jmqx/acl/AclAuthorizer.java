package com.jmqx.acl;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AclAuthorizer {
    AclDecision authorize(AclRequest request);

    default boolean isAllowed(AclRequest request) {
        return authorize(request) == AclDecision.ALLOW;
    }
}
