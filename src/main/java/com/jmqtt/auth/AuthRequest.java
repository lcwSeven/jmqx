package com.jmqtt.auth;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AuthRequest {
    private final String username;
    private final String password;

    public AuthRequest(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
