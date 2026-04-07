package com.jmqx.protocol;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface ClientAuthenticator {

    boolean authenticate(String clientId, String username, String password);

    ClientAuthenticator ALLOW_ALL = (clientId, username, password) -> true;
}
