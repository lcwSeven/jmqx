package com.jmqx.admin;

import java.util.List;

/**
 * 管理端数据上报器。
 *
 * @author liucaiwen
 * @since 2026-04-10
 */
public interface AdminReporter {

    AdminReporter NOOP = new AdminReporter() {
    };

    default void upsertClientSession(String clientId,
                                     String nodeId,
                                     String clientIp,
                                     int keepAliveSeconds,
                                     String connectionType,
                                     String username,
                                     long connectedAtEpochMs) {
    }

    default void removeClientSession(String clientId) {
    }

    default void upsertClientSubscriptions(String clientId, List<String> topics) {
    }

    default void upsertNodeMetrics(String nodeId, String nodeIp, long inboundBytes, long outboundBytes, int connectedClients, long reportTime) {
    }

    default void shutdown() {
    }
}
