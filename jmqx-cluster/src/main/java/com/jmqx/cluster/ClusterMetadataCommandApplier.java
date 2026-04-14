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
    public static final String RETAINED_NAMESPACE = "retained.message";
    public static final String ADMIN_SECURITY_NAMESPACE = "admin.security.config";
    public static final String ADMIN_CLUSTER_NAMESPACE = "admin.cluster.config";
    public static final String ADMIN_BRIDGE_NAMESPACE = "admin.bridge.config";
    public static final String ADMIN_BUILT_IN_USER_NAMESPACE = "admin.auth.built-in-user";
    public static final String ADMIN_BLACKLIST_NAMESPACE = "admin.security.blacklist";

    private final String localNodeId;
    private final RouteSubscriptionCommandHandler routeHandler;
    private final ClientOnlineCommandHandler clientOnlineHandler;
    private final RetainedCommandHandler retainedCommandHandler;
    private final AdminSecurityConfigCommandHandler adminSecurityConfigHandler;
    private final AdminClusterConfigCommandHandler adminClusterConfigHandler;
    private final AdminBridgeConfigCommandHandler adminBridgeConfigHandler;
    private final AdminBuiltInUserCommandHandler adminBuiltInUserCommandHandler;
    private final AdminBlacklistCommandHandler adminBlacklistCommandHandler;

    public ClusterMetadataCommandApplier(
            String localNodeId,
            RouteSubscriptionCommandHandler routeHandler,
            ClientOnlineCommandHandler clientOnlineHandler,
            RetainedCommandHandler retainedCommandHandler,
            AdminSecurityConfigCommandHandler adminSecurityConfigHandler,
            AdminClusterConfigCommandHandler adminClusterConfigHandler,
            AdminBridgeConfigCommandHandler adminBridgeConfigHandler,
            AdminBuiltInUserCommandHandler adminBuiltInUserCommandHandler,
            AdminBlacklistCommandHandler adminBlacklistCommandHandler
    ) {
        this.localNodeId = localNodeId;
        this.routeHandler = Objects.requireNonNull(routeHandler, "routeHandler");
        this.clientOnlineHandler = Objects.requireNonNull(clientOnlineHandler, "clientOnlineHandler");
        this.retainedCommandHandler = Objects.requireNonNull(retainedCommandHandler, "retainedCommandHandler");
        this.adminSecurityConfigHandler = Objects.requireNonNull(adminSecurityConfigHandler, "adminSecurityConfigHandler");
        this.adminClusterConfigHandler = Objects.requireNonNull(adminClusterConfigHandler, "adminClusterConfigHandler");
        this.adminBridgeConfigHandler = Objects.requireNonNull(adminBridgeConfigHandler, "adminBridgeConfigHandler");
        this.adminBuiltInUserCommandHandler = Objects.requireNonNull(adminBuiltInUserCommandHandler, "adminBuiltInUserCommandHandler");
        this.adminBlacklistCommandHandler = Objects.requireNonNull(adminBlacklistCommandHandler, "adminBlacklistCommandHandler");
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
            return;
        }
        if (RETAINED_NAMESPACE.equals(command.namespace())) {
            retainedCommandHandler.apply(logIndex, localNodeId, command);
            return;
        }
        if (ADMIN_SECURITY_NAMESPACE.equals(command.namespace())) {
            adminSecurityConfigHandler.apply(localNodeId, command);
            return;
        }
        if (ADMIN_CLUSTER_NAMESPACE.equals(command.namespace())) {
            adminClusterConfigHandler.apply(localNodeId, command);
            return;
        }
        if (ADMIN_BRIDGE_NAMESPACE.equals(command.namespace())) {
            adminBridgeConfigHandler.apply(localNodeId, command);
            return;
        }
        if (ADMIN_BUILT_IN_USER_NAMESPACE.equals(command.namespace())) {
            adminBuiltInUserCommandHandler.apply(localNodeId, command);
            return;
        }
        if (ADMIN_BLACKLIST_NAMESPACE.equals(command.namespace())) {
            adminBlacklistCommandHandler.apply(localNodeId, command);
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

    /**
     * Retained 命令处理器。
     */
    @FunctionalInterface
    public interface RetainedCommandHandler {
        void apply(long logIndex, String localNodeId, MetadataCommand command);
    }

    @FunctionalInterface
    public interface AdminSecurityConfigCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    @FunctionalInterface
    public interface AdminClusterConfigCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    @FunctionalInterface
    public interface AdminBridgeConfigCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    @FunctionalInterface
    public interface AdminBuiltInUserCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    @FunctionalInterface
    public interface AdminBlacklistCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }
}
