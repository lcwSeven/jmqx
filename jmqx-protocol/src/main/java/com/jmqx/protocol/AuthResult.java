package com.jmqx.protocol;

/**
 * @author liucaiwen
 * @date 2026/4/13
 */
public record AuthResult(AuthDecision decision, boolean superuser) {
    public static AuthResult allow() {
        return new AuthResult(AuthDecision.ALLOW, false);
    }

    public static AuthResult allow(boolean superuser) {
        return new AuthResult(AuthDecision.ALLOW, superuser);
    }

    public static AuthResult deny() {
        return new AuthResult(AuthDecision.DENY, false);
    }

    public static AuthResult notFound() {
        return new AuthResult(AuthDecision.NOT_FOUND, false);
    }
}
