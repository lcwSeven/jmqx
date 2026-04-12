package com.jmqx.cluster;

import java.util.Objects;

/**
 * 集群元数据命令分发器。
 * 仅负责按 namespace 分发命令
 *
 * @author liucaiwen
 * @date 2026/4/11
 */
public class ClusterMetadataCommandApplier {
    public static final String SUBSCRIPTION_NAMESPACE = "route.subscription";
    public static final String SESSION_NAMESPACE = "session.client";

    private final String localNodeId;
    private final RouteSubscriptionCommandHandler routeHandler;
    private final ClientOnlineCommandHandler clientOnlineHandler;

    public ClusterMetadataCommandApplier(
            String localNodeId,
            RouteSubscriptionCommandHandler routeHandler,
            ClientOnlineCommandHandler clientOnlineHandler
    ) {
        this.localNodeId = localNodeId;
        this.routeHandler = Objects.requireNonNull(routeHandler, "routeHandler");
        this.clientOnlineHandler = Objects.requireNonNull(clientOnlineHandler, "clientOnlineHandler");
    }

    /**
     * 应用一条元数据命令。
     */
    public void apply(long logIndex, MetadataCommand command) {
        if (command == null) {
            return;
        }
        if (SUBSCRIPTION_NAMESPACE.equals(command.namespace())) {
            routeHandler.apply(logIndex, command);
            return;
        }
        if (SESSION_NAMESPACE.equals(command.namespace())) {
            clientOnlineHandler.apply(localNodeId, command);
        }
    }

    /**
     * 路由订阅命令处理器。
     */
    @FunctionalInterface
    public interface RouteSubscriptionCommandHandler {
        void apply(long logIndex, MetadataCommand command);
    }

    /**
     * 客户端上线命令处理器。
     */
    @FunctionalInterface
    public interface ClientOnlineCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }
}
