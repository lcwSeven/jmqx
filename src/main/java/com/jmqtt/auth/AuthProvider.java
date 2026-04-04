package com.jmqtt.auth;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public interface AuthProvider {
    boolean authenticate(AuthRequest request);
}
