package com.jmqtt.session;

import io.netty.channel.Channel;

import java.time.Instant;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class ClientSession {
    private final String clientId;
    private final Channel channel;
    private final boolean cleanSession;
    private final String username;
    private final String serviceNodeIp;
    private final int keepAliveSeconds;
    private final Instant connectedAt;

    public ClientSession(
        String clientId,
        Channel channel,
        boolean cleanSession,
        String username,
        String serviceNodeIp,
        int keepAliveSeconds,
        Instant connectedAt
    ) {
        this.clientId = clientId;
        this.channel = channel;
        this.cleanSession = cleanSession;
        this.username = username;
        this.serviceNodeIp = serviceNodeIp;
        this.keepAliveSeconds = keepAliveSeconds;
        this.connectedAt = connectedAt;
    }

    public String getClientId() {
        return clientId;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public String getUsername() {
        return username;
    }

    public String getServiceNodeIp() {
        return serviceNodeIp;
    }

    public int getKeepAliveSeconds() {
        return keepAliveSeconds;
    }

    public Instant getConnectedAt() {
        return connectedAt;
    }
}
