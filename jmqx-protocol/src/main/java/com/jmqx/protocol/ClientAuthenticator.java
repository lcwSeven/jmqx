package com.jmqx.protocol;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface ClientAuthenticator {

    default boolean authenticate(String clientId, String username, String password) {
        return authenticateResult(clientId, username, password).decision() == AuthDecision.ALLOW;
    }

    AuthResult authenticateResult(String clientId, String username, String password);

    ClientAuthenticator ALLOW_ALL = (clientId, username, password) -> AuthResult.allow();
}
