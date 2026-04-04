package com.jmqtt.acl;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AclRequest {
    private final String clientId;
    private final String username;
    private final String topic;
    private final AclAction action;

    public AclRequest(String clientId, String username, String topic, AclAction action) {
        this.clientId = clientId;
        this.username = username;
        this.topic = topic;
        this.action = action;
    }

    public String getClientId() {
        return clientId;
    }

    public String getUsername() {
        return username;
    }

    public String getTopic() {
        return topic;
    }

    public AclAction getAction() {
        return action;
    }
}
