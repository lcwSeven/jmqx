package com.jmqx.auth;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AuthProviderPlugin {
    String type();

    AuthProvider create(AuthProperties properties);
}
