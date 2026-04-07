package com.jmqx.auth;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AuthProviderPlugin {
    /**
     * type
     * @return return
     */
    String type();

    AuthProvider create(AuthProperties properties);
}
