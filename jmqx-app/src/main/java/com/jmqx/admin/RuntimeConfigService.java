package com.jmqx.admin;

import com.jmqx.acl.AclAuthorizerFactory;
import com.jmqx.acl.AclProperties;
import com.jmqx.acl.ReloadableAclAuthorizer;
import com.jmqx.auth.AuthProperties;
import com.jmqx.auth.AuthProviderFactory;
import com.jmqx.auth.ReloadableAuthProvider;

/**
 * @author liucaiwen
 * @date 2026/4/7
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
        String authChain,
        Integer authCacheMillis,
        String authHttpUrl,
        Integer authHttpTimeoutMs,
        String authFilePath,
        String authRedisHost,
        Integer authRedisPort,
        String authRedisPassword,
        Integer authRedisDb,
        String authRedisKeyPrefix,
        Integer authRedisTimeoutMs,
        String authDbDriver,
        String authDbUrl,
        String authDbUser,
        String authDbPassword,
        String authDbQuery,
        String aclType,
        Integer aclCacheMillis,
        Boolean aclDefaultAllow,
        String aclHttpUrl,
        Integer aclHttpTimeoutMs,
        String aclRedisHost,
        Integer aclRedisPort,
        String aclRedisPassword,
        Integer aclRedisDb,
        String aclRedisKeyPrefix,
        Integer aclRedisTimeoutMs,
        String aclFilePath
    ) {
        boolean authChanged = false;
        boolean aclChanged = false;

        if (authType != null && !authType.isBlank()) {
            applyAuthType(authType);
            authChanged = true;
        }
        if (authChain != null) {
            applyAuthChain(authChain);
            authChanged = true;
        }
        if (authCacheMillis != null) {
            authProperties.setCacheMillis(Math.max(authCacheMillis, 0));
            authChanged = true;
        }
        if (authHttpUrl != null) {
            authProperties.setHttpUrl(authHttpUrl);
            authChanged = true;
        }
        if (authHttpTimeoutMs != null) {
            authProperties.setHttpTimeoutMs(Math.max(authHttpTimeoutMs, 0));
            authChanged = true;
        }
        if (authFilePath != null) {
            authProperties.setFilePath(authFilePath);
            authChanged = true;
        }
        if (authRedisHost != null) {
            authProperties.setRedisHost(authRedisHost);
            authChanged = true;
        }
        if (authRedisPort != null) {
            authProperties.setRedisPort(Math.max(authRedisPort, 0));
            authChanged = true;
        }
        if (authRedisPassword != null) {
            authProperties.setRedisPassword(authRedisPassword);
            authChanged = true;
        }
        if (authRedisDb != null) {
            authProperties.setRedisDb(Math.max(authRedisDb, 0));
            authChanged = true;
        }
        if (authRedisKeyPrefix != null) {
            authProperties.setRedisKeyPrefix(authRedisKeyPrefix);
            authChanged = true;
        }
        if (authRedisTimeoutMs != null) {
            authProperties.setRedisTimeoutMs(Math.max(authRedisTimeoutMs, 0));
            authChanged = true;
        }
        if (authDbDriver != null) {
            authProperties.setDbDriver(authDbDriver);
            authChanged = true;
        }
        if (authDbUrl != null) {
            authProperties.setDbUrl(authDbUrl);
            authChanged = true;
        }
        if (authDbUser != null) {
            authProperties.setDbUser(authDbUser);
            authChanged = true;
        }
        if (authDbPassword != null) {
            authProperties.setDbPassword(authDbPassword);
            authChanged = true;
        }
        if (authDbQuery != null) {
            authProperties.setDbQuery(authDbQuery);
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
        if (aclDefaultAllow != null) {
            aclProperties.setDefaultAllow(aclDefaultAllow);
            aclChanged = true;
        }
        if (aclHttpUrl != null) {
            aclProperties.setHttpUrl(aclHttpUrl);
            aclChanged = true;
        }
        if (aclHttpTimeoutMs != null) {
            aclProperties.setHttpTimeoutMs(Math.max(aclHttpTimeoutMs, 0));
            aclChanged = true;
        }
        if (aclRedisHost != null) {
            aclProperties.setRedisHost(aclRedisHost);
            aclChanged = true;
        }
        if (aclRedisPort != null) {
            aclProperties.setRedisPort(Math.max(aclRedisPort, 0));
            aclChanged = true;
        }
        if (aclRedisPassword != null) {
            aclProperties.setRedisPassword(aclRedisPassword);
            aclChanged = true;
        }
        if (aclRedisDb != null) {
            aclProperties.setRedisDb(Math.max(aclRedisDb, 0));
            aclChanged = true;
        }
        if (aclRedisKeyPrefix != null) {
            aclProperties.setRedisKeyPrefix(aclRedisKeyPrefix);
            aclChanged = true;
        }
        if (aclRedisTimeoutMs != null) {
            aclProperties.setRedisTimeoutMs(Math.max(aclRedisTimeoutMs, 0));
            aclChanged = true;
        }
        if (aclFilePath != null) {
            aclProperties.setFilePath(aclFilePath);
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
        String chain = authProperties.getChain();
        if (chain != null && !chain.isBlank()) {
            return chain;
        }
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

    private void applyAuthType(String authTypeRaw) {
        String value = authTypeRaw.trim();
        if (value.contains(",")) {
            authProperties.setChain(value);
            String[] parts = value.split(",");
            authProperties.setType(parts.length == 0 ? "allow_all" : parts[0].trim());
            return;
        }
        authProperties.setType(value);
        authProperties.setChain("");
    }

    private void applyAuthChain(String authChainRaw) {
        String value = authChainRaw == null ? "" : authChainRaw.trim();
        authProperties.setChain(value);
        if (!value.isBlank()) {
            String[] parts = value.split(",");
            if (parts.length > 0) {
                authProperties.setType(parts[0].trim());
            }
        }
    }
}
