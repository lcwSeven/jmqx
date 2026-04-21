package com.jmqx.cluster;

import java.util.Objects;

/**
 * Dispatches replicated metadata commands to the matching domain handler.
 * <p>
 * This class intentionally stays small: it does not interpret payloads, keep
 * state, or implement business logic on its own. Its only job is to examine
 * the command namespace and forward the command to the handler that owns that
 * namespace.
 * <p>
 * Different handlers receive slightly different arguments because some domains
 * need extra context:
 * <ul>
 *   <li>{@code logIndex} is passed to handlers that need the replicated log
 *   position for ordering or idempotency.</li>
 *   <li>{@code localNodeId} is passed to handlers that need to know which node
 *   is applying the command locally.</li>
 * </ul>
 */
public class ClusterMetadataCommandApplier {
    /** Namespace for replicated subscription route changes. */
    public static final String SUBSCRIPTION_NAMESPACE = "route.subscription";
    /** Namespace for replicated client session lifecycle commands. */
    public static final String SESSION_NAMESPACE = "session.client";
    /** Namespace for retained message replication commands. */
    public static final String RETAINED_NAMESPACE = "retained.message";
    /** Namespace for admin-side security configuration changes. */
    public static final String ADMIN_SECURITY_NAMESPACE = "admin.security.config";
    /** Namespace for admin-side cluster deployment configuration changes. */
    public static final String ADMIN_CLUSTER_NAMESPACE = "admin.cluster.config";
    /** Namespace for admin-side bridge configuration changes. */
    public static final String ADMIN_BRIDGE_NAMESPACE = "admin.bridge.config";
    /** Namespace for built-in database user mutations from the admin console. */
    public static final String ADMIN_BUILT_IN_USER_NAMESPACE = "admin.auth.built-in-user";
    /** Namespace for admin-managed blacklist mutations. */
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
     * Applies one replicated metadata command.
     * <p>
     * Unknown namespaces are ignored on purpose so a node can safely skip
     * command types it does not understand yet.
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
     * Handles subscription route mutations.
     */
    @FunctionalInterface
    public interface RouteSubscriptionCommandHandler {
        void apply(long logIndex, MetadataCommand command);
    }

    /**
     * Handles client session lifecycle mutations.
     */
    @FunctionalInterface
    public interface ClientOnlineCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    /**
     * Handles retained message mutations.
     */
    @FunctionalInterface
    public interface RetainedCommandHandler {
        void apply(long logIndex, String localNodeId, MetadataCommand command);
    }

    /** Handles admin-side security configuration mutations. */
    @FunctionalInterface
    public interface AdminSecurityConfigCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    /** Handles admin-side cluster deployment configuration mutations. */
    @FunctionalInterface
    public interface AdminClusterConfigCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    /** Handles admin-side bridge configuration mutations. */
    @FunctionalInterface
    public interface AdminBridgeConfigCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    /** Handles built-in database user mutations from the admin console. */
    @FunctionalInterface
    public interface AdminBuiltInUserCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }

    /** Handles admin-managed blacklist mutations. */
    @FunctionalInterface
    public interface AdminBlacklistCommandHandler {
        void apply(String localNodeId, MetadataCommand command);
    }
}
