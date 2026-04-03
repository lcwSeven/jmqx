package com.jmqtt.protocol;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface ClientAuthenticator {
    boolean authenticate(String username, String password);

    ClientAuthenticator ALLOW_ALL = (username, password) -> true;
}
