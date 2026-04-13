package com.jmqx.session;

import io.netty.channel.Channel;

import java.time.Instant;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public record ClientSession(String clientId,
                            Channel channel,
                            String connectionType,
                            boolean cleanStart,
                            long sessionExpiryIntervalSeconds,
                            String username,
                            String serviceNodeIp,
                            int keepAliveSeconds,
                            boolean superuser,
                            Instant connectedAt) {
}
