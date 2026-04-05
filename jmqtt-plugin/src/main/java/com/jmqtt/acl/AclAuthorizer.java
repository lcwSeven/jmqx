package com.jmqtt.acl;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AclAuthorizer {
    boolean isAllowed(AclRequest request);
}
