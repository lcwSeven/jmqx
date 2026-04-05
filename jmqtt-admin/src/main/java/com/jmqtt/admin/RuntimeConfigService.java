package com.jmqtt.admin;

import com.jmqtt.acl.AclAuthorizerFactory;
import com.jmqtt.acl.AclProperties;
import com.jmqtt.acl.ReloadableAclAuthorizer;
import com.jmqtt.auth.AuthProperties;
import com.jmqtt.auth.AuthProviderFactory;
import com.jmqtt.auth.ReloadableAuthProvider;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class RuntimeConfigService {
    private final AuthProperties authProperties;
    private final AclProperties aclProperties;
    private final ReloadableAuthProvider reloadableAuthProvider;
    private final ReloadableAclAuthorizer reloadableAclAuthorizer;

    public RuntimeConfigService(
        AuthProperties authProperties,
        AclProperties aclProperties,
        ReloadableAuthProvider reloadableAuthProvider,
        ReloadableAclAuthorizer reloadableAclAuthorizer
    ) {
        this.authProperties = authProperties;
        this.aclProperties = aclProperties;
        this.reloadableAuthProvider = reloadableAuthProvider;
        this.reloadableAclAuthorizer = reloadableAclAuthorizer;
    }

    public synchronized void update(
        String authType,
        Integer authCacheMillis,
        String aclType,
        Integer aclCacheMillis
    ) {
        boolean authChanged = false;
        boolean aclChanged = false;

        if (authType != null && !authType.isBlank()) {
            authProperties.setType(authType);
            authChanged = true;
        }
        if (authCacheMillis != null) {
            authProperties.setCacheMillis(Math.max(authCacheMillis, 0));
            authChanged = true;
        }
        if (aclType != null && !aclType.isBlank()) {
            aclProperties.setType(aclType);
            aclChanged = true;
        }
        if (aclCacheMillis != null) {
            aclProperties.setCacheMillis(Math.max(aclCacheMillis, 0));
            aclChanged = true;
        }

        if (authChanged) {
            reloadableAuthProvider.setDelegate(AuthProviderFactory.create(authProperties));
        }
        if (aclChanged) {
            reloadableAclAuthorizer.setDelegate(AclAuthorizerFactory.create(aclProperties));
        }
    }

    public synchronized String getAuthType() {
        return authProperties.getType();
    }

    public synchronized int getAuthCacheMillis() {
        return authProperties.getCacheMillis();
    }

    public synchronized String getAclType() {
        return aclProperties.getType();
    }

    public synchronized int getAclCacheMillis() {
        return aclProperties.getCacheMillis();
    }
}
