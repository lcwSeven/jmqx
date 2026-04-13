package com.jmqx.protocol;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface ClientAuthenticator {

    AuthResult authenticateResult(String clientId, String username, String password);

    default void evictCache(String clientId, String username) {
    }

}
