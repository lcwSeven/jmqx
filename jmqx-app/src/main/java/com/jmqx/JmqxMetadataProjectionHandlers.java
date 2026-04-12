package com.jmqx;

import com.jmqx.cluster.ClusterMetadataCommandApplier;
import com.jmqx.cluster.MetadataCommand;
import com.jmqx.router.global.GlobalSubscriptionEvent;
import com.jmqx.router.global.GlobalSubscriptionRegistry;
import com.jmqx.session.SessionRegistry;
import io.netty.util.AttributeKey;

import java.util.logging.Logger;

/**
 * JMQX 应用层元数据投影处理器。
 * 把 cluster 模块分发过来的命令投影到本地读模型（路由表、会话表）。
 *
 * @author liucaiwen
 * @date 2026/4/11
 */
public class JmqxMetadataProjectionHandlers {
    private static final Logger LOG = Logger.getLogger(JmqxMetadataProjectionHandlers.class.getName());
    private static final String OP_ONLINE = "online";
    private static final AttributeKey<Boolean> GRACEFUL_DISCONNECT = AttributeKey.valueOf("jmqx.gracefulDisconnect");

    private final GlobalSubscriptionRegistry globalSubscriptionRegistry;
    private final SessionRegistry sessionRegistry;

    public JmqxMetadataProjectionHandlers(
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            SessionRegistry sessionRegistry
    ) {
        this.globalSubscriptionRegistry = globalSubscriptionRegistry;
        this.sessionRegistry = sessionRegistry;
    }

    /**
     * 应用全局订阅路由命令。
     */
    public void applyRouteSubscriptionCommand(long logIndex, MetadataCommand command) {
        if (command == null || globalSubscriptionRegistry == null) {
            return;
        }
        String topicFilter = command.key();
        if (topicFilter == null || topicFilter.isBlank()) {
            return;
        }
        String sourceNode = command.sourceNodeId();
        if (sourceNode == null || sourceNode.isBlank()) {
            return;
        }
        String sharedGroup = (command.value() == null || command.value().isBlank()) ? null : command.value();
        if ("register".equals(command.operation())) {
            globalSubscriptionRegistry.apply(GlobalSubscriptionEvent.register(logIndex, sourceNode, topicFilter, sharedGroup));
            return;
        }
        if ("unregister".equals(command.operation())) {
            globalSubscriptionRegistry.apply(GlobalSubscriptionEvent.unregister(logIndex, sourceNode, topicFilter, sharedGroup));
        }
    }

    /**
     * 应用客户端上线命令。
     * 当其他节点声明某个 clientId 已上线时，本节点若存在同 clientId 会话则主动断开，保证全局唯一。
     */
    public void applyClientOnlineCommand(String localNodeId, MetadataCommand command) {
        if (sessionRegistry == null || command == null) {
            return;
        }
        if (!ClusterMetadataCommandApplier.SESSION_NAMESPACE.equals(command.namespace())) {
            return;
        }
        if (!OP_ONLINE.equals(command.operation())) {
            return;
        }
        String clientId = command.key();
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        String sourceNodeId = command.sourceNodeId();
        if (sourceNodeId == null || sourceNodeId.isBlank() || sourceNodeId.equals(localNodeId)) {
            return;
        }
        long eventConnectedAtMs = parseLong(command.value(), 0L);
        sessionRegistry.get(clientId).ifPresent(session -> {
            if (session.channel() == null || !session.channel().isActive()) {
                return;
            }
            long localConnectedAtMs = session.connectedAt() == null ? 0L : session.connectedAt().toEpochMilli();
            // 忽略明显过期的事件，避免重放老日志误踢当前新会话。
            if (eventConnectedAtMs > 0L && localConnectedAtMs > eventConnectedAtMs) {
                return;
            }
            session.channel().attr(GRACEFUL_DISCONNECT).set(true);
            session.channel().close();
            LOG.info(() -> "[CLUSTER] kicked duplicated client session, clientId=" + clientId
                    + ", localNodeId=" + localNodeId + ", ownerNodeId=" + sourceNodeId);
        });
    }

    private static long parseLong(String value, long defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ignore) {
            return defaultValue;
        }
    }
}
