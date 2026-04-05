package com.jmqtt.auth;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AuthRequest {
    private final String clientId;
    private final String username;
    private final String password;

    public AuthRequest(String clientId, String username, String password) {
        this.clientId = clientId;
        this.username = username;
        this.password = password;
    }

    public String getClientId() {
        return clientId;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
