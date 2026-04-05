package com.jmqtt.auth;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AllowAllAuthProvider implements AuthProvider {
    @Override
    public boolean authenticate(AuthRequest request) {
        return true;
    }
}
