package com.jmqx;

import com.jmqx.admin.embedded.AdminConfigCodec;
import com.jmqx.admin.embedded.AdminStateRepository;
import com.jmqx.admin.embedded.BuiltInDatabaseUserService;
import com.jmqx.admin.embedded.EmbeddedAdminStateStore;
import com.jmqx.cluster.ClusterMetadataCommandApplier;
import com.jmqx.cluster.MetadataCommand;
import com.jmqx.protocol.ClientBlacklist;
import com.jmqx.protocol.ClientAuthenticator;
import com.jmqx.router.global.GlobalSubscriptionEvent;
import com.jmqx.router.global.GlobalSubscriptionRegistry;
import com.jmqx.session.SessionRegistry;
import com.jmqx.store.retained.RetainedMessage;
import com.jmqx.store.retained.RetainedMessageStore;
import io.netty.util.AttributeKey;

import java.util.Base64;
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
    private static final String OP_KICK = "kick";
    private static final String OP_AUTH_CACHE_EVICT = "auth_cache_evict";
    private static final String OP_RETAINED_UPSERT = "upsert";
    private static final String OP_RETAINED_REMOVE = "remove";
    private static final AttributeKey<Boolean> GRACEFUL_DISCONNECT = AttributeKey.valueOf("jmqx.gracefulDisconnect");

    private final GlobalSubscriptionRegistry globalSubscriptionRegistry;
    private final String localClusterId;
    private final SessionRegistry sessionRegistry;
    private final RetainedMessageStore retainedMessageStore;
    private final AdminStateRepository adminStateRepository;
    private final BuiltInDatabaseUserService builtInDatabaseUserService;
    private final ClientAuthenticator clientAuthenticator;
    private final ClientBlacklist clientBlacklist;
    private final AdminSecurityConfigApplier adminSecurityConfigApplier;
    private final AdminClusterConfigApplier adminClusterConfigApplier;
    private final AdminBridgeConfigApplier adminBridgeConfigApplier;

    public JmqxMetadataProjectionHandlers(
            GlobalSubscriptionRegistry globalSubscriptionRegistry,
            String localClusterId,
            SessionRegistry sessionRegistry,
            RetainedMessageStore retainedMessageStore,
            AdminStateRepository adminStateRepository,
            BuiltInDatabaseUserService builtInDatabaseUserService,
            ClientAuthenticator clientAuthenticator,
            ClientBlacklist clientBlacklist,
            AdminSecurityConfigApplier adminSecurityConfigApplier,
            AdminClusterConfigApplier adminClusterConfigApplier,
            AdminBridgeConfigApplier adminBridgeConfigApplier
    ) {
        this.globalSubscriptionRegistry = globalSubscriptionRegistry;
        this.localClusterId = localClusterId == null || localClusterId.isBlank() ? "default" : localClusterId.trim();
        this.sessionRegistry = sessionRegistry;
        this.retainedMessageStore = retainedMessageStore;
        this.adminStateRepository = adminStateRepository;
        this.builtInDatabaseUserService = builtInDatabaseUserService;
        this.clientAuthenticator = clientAuthenticator;
        this.clientBlacklist = clientBlacklist == null ? ClientBlacklist.NOOP : clientBlacklist;
        this.adminSecurityConfigApplier = adminSecurityConfigApplier;
        this.adminClusterConfigApplier = adminClusterConfigApplier;
        this.adminBridgeConfigApplier = adminBridgeConfigApplier;
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
            if (OP_AUTH_CACHE_EVICT.equals(command.operation())) {
                if (clientAuthenticator != null) {
                    clientAuthenticator.evictCache(command.key(), command.value());
                }
                return;
            }
            if (OP_KICK.equals(command.operation())) {
                kickLocalSession(command.key(), localNodeId, command.sourceNodeId(), "admin");
            }
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

    public void applyAdminBlacklistCommand(String localNodeId, MetadataCommand command) {
        if (adminStateRepository == null || command == null) {
            return;
        }
        if (!ClusterMetadataCommandApplier.ADMIN_BLACKLIST_NAMESPACE.equals(command.namespace())) {
            return;
        }
        String clusterId = command.key();
        if (clusterId == null || clusterId.isBlank()) {
            return;
        }
        EmbeddedAdminStateStore.BlacklistEntry entry = AdminConfigCodec.decodeBlacklistEntryFromString(command.value());
        if (entry == null || entry.value() == null || entry.value().isBlank()) {
            return;
        }
        if ("upsert".equals(command.operation())) {
            adminStateRepository.upsertBlacklistEntry(clusterId, entry);
            if (localClusterId.equals(clusterId)) {
                clientBlacklist.upsert(entry.type(), entry.value());
                disconnectBlacklistedSessions(entry);
            }
            return;
        }
        if ("delete".equals(command.operation())) {
            adminStateRepository.removeBlacklistEntry(clusterId, entry.type(), entry.value());
            if (localClusterId.equals(clusterId)) {
                clientBlacklist.remove(entry.type(), entry.value());
            }
        }
    }

    /**
     * 应用 retained 命令，确保各节点 retained 存储一致。
     */
    public void applyRetainedCommand(long logIndex, String localNodeId, MetadataCommand command) {
        if (retainedMessageStore == null || command == null) {
            return;
        }
        if (!ClusterMetadataCommandApplier.RETAINED_NAMESPACE.equals(command.namespace())) {
            return;
        }
        String topic = command.key();
        if (topic == null || topic.isBlank()) {
            return;
        }
        String operation = command.operation();
        if (OP_RETAINED_REMOVE.equals(operation)) {
            retainedMessageStore.saveOrRemove(new RetainedMessage(topic, new byte[0], 0, true));
            return;
        }
        if (!OP_RETAINED_UPSERT.equals(operation)) {
            return;
        }
        String value = command.value();
        if (value == null || value.isBlank()) {
            return;
        }
        int split = value.indexOf('|');
        if (split <= 0 || split >= value.length() - 1) {
            return;
        }
        int qos = (int) parseLong(value.substring(0, split), 0L);
        String base64Payload = value.substring(split + 1);
        byte[] payload;
        try {
            payload = Base64.getDecoder().decode(base64Payload);
        } catch (IllegalArgumentException exception) {
            LOG.warning("[CLUSTER] retained payload decode failed topic=" + topic + ", error=" + exception.getMessage());
            return;
        }
        retainedMessageStore.saveOrRemove(new RetainedMessage(topic, payload, qos, true));
    }

    public void applyAdminSecurityConfigCommand(String localNodeId, MetadataCommand command) {
        if (adminStateRepository == null || adminSecurityConfigApplier == null || command == null) {
            return;
        }
        if (!ClusterMetadataCommandApplier.ADMIN_SECURITY_NAMESPACE.equals(command.namespace())) {
            return;
        }
        String clusterId = command.key();
        if (clusterId == null || clusterId.isBlank()) {
            return;
        }
        EmbeddedAdminStateStore.SecurityConfig config = AdminConfigCodec.decodeSecurityConfigFromString(command.value());
        if (config == null) {
            return;
        }
        adminStateRepository.setSecurityConfig(clusterId, config);
        adminSecurityConfigApplier.apply(clusterId, config);
    }

    public void applyAdminClusterConfigCommand(String localNodeId, MetadataCommand command) {
        if (adminStateRepository == null || adminClusterConfigApplier == null || command == null) {
            return;
        }
        if (!ClusterMetadataCommandApplier.ADMIN_CLUSTER_NAMESPACE.equals(command.namespace())) {
            return;
        }
        String clusterId = command.key();
        if (clusterId == null || clusterId.isBlank()) {
            return;
        }
        EmbeddedAdminStateStore.ClusterConfig config = AdminConfigCodec.decodeClusterConfigFromString(command.value());
        if (config == null) {
            return;
        }
        adminStateRepository.setClusterConfig(clusterId, config);
        adminClusterConfigApplier.apply(clusterId, config);
    }

    public void applyAdminBridgeConfigCommand(String localNodeId, MetadataCommand command) {
        if (adminStateRepository == null || adminBridgeConfigApplier == null || command == null) {
            return;
        }
        if (!ClusterMetadataCommandApplier.ADMIN_BRIDGE_NAMESPACE.equals(command.namespace())) {
            return;
        }
        String clusterId = command.key();
        if (clusterId == null || clusterId.isBlank()) {
            return;
        }
        EmbeddedAdminStateStore.BridgeConfig config = AdminConfigCodec.decodeBridgeConfigFromString(command.value());
        if (config == null) {
            return;
        }
        adminStateRepository.setBridgeConfig(clusterId, config);
        adminBridgeConfigApplier.apply(clusterId, config);
    }

    public void applyBuiltInUserCommand(String localNodeId, MetadataCommand command) {
        if (builtInDatabaseUserService == null || command == null) {
            return;
        }
        if (!ClusterMetadataCommandApplier.ADMIN_BUILT_IN_USER_NAMESPACE.equals(command.namespace())) {
            return;
        }
        String operation = command.operation();
        if ("upsert".equals(operation)) {
            String userId = command.key();
            String encodedCredential = command.value();
            if (userId == null || userId.isBlank() || encodedCredential == null || encodedCredential.isBlank()) {
                return;
            }
            builtInDatabaseUserService.upsertEncodedUser(userId, encodedCredential);
            return;
        }
        if ("delete".equals(operation)) {
            builtInDatabaseUserService.deleteUser(command.key());
            return;
        }
        if ("clear".equals(operation)) {
            builtInDatabaseUserService.deleteAllUsers();
        }
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

    private void disconnectBlacklistedSessions(EmbeddedAdminStateStore.BlacklistEntry entry) {
        if (sessionRegistry == null || entry == null) {
            return;
        }
        for (var session : sessionRegistry.list()) {
            if (session == null || session.channel() == null || !session.channel().isActive()) {
                continue;
            }
            if (matchesBlacklist(entry, session)) {
                session.channel().attr(GRACEFUL_DISCONNECT).set(true);
                session.channel().close();
                LOG.info(() -> "[CLUSTER] disconnected blacklisted client, type=" + entry.type()
                        + ", value=" + entry.value());
            }
        }
    }

    private boolean matchesBlacklist(EmbeddedAdminStateStore.BlacklistEntry entry, com.jmqx.session.ClientSession session) {
        if (ClientBlacklist.TYPE_IP.equalsIgnoreCase(entry.type())) {
            return entry.value().equals(resolveClientIp(session));
        }
        return entry.value().equals(session.clientId());
    }

    private void kickLocalSession(String clientId, String localNodeId, String sourceNodeId, String reason) {
        if (sessionRegistry == null || clientId == null || clientId.isBlank()) {
            return;
        }
        sessionRegistry.get(clientId).ifPresent(session -> {
            if (session.channel() == null || !session.channel().isActive()) {
                return;
            }
            session.channel().attr(GRACEFUL_DISCONNECT).set(true);
            session.channel().close();
            LOG.info(() -> "[CLUSTER] kicked client session, clientId=" + clientId
                    + ", localNodeId=" + localNodeId + ", sourceNodeId=" + sourceNodeId + ", reason=" + reason);
        });
    }

    private static String resolveClientIp(com.jmqx.session.ClientSession session) {
        if (session == null || session.channel() == null || !(session.channel().remoteAddress() instanceof java.net.InetSocketAddress address)) {
            return "unknown";
        }
        if (address.getAddress() != null) {
            return address.getAddress().getHostAddress();
        }
        return address.getHostString() == null ? "unknown" : address.getHostString();
    }

    @FunctionalInterface
    public interface AdminSecurityConfigApplier {
        void apply(String clusterId, EmbeddedAdminStateStore.SecurityConfig config);
    }

    @FunctionalInterface
    public interface AdminClusterConfigApplier {
        void apply(String clusterId, EmbeddedAdminStateStore.ClusterConfig config);
    }

    @FunctionalInterface
    public interface AdminBridgeConfigApplier {
        void apply(String clusterId, EmbeddedAdminStateStore.BridgeConfig config);
    }
}
