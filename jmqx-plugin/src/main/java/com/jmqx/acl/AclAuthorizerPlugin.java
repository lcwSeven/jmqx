package com.jmqx.acl;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AclAuthorizerPlugin {
    String type();

    AclAuthorizer create(AclProperties properties);
}
