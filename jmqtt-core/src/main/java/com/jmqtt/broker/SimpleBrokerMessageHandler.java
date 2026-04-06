package com.jmqtt.broker;

import com.jmqtt.acl.AclAction;
import com.jmqtt.acl.AclAuthorizer;
import com.jmqtt.acl.AclRequest;
import com.jmqtt.cluster.BrokerClusterReceiver;
import com.jmqtt.cluster.ClusterPublishMessage;
import com.jmqtt.cluster.ClusterReplicator;
import com.jmqtt.common.SharedSubscription;
import com.jmqtt.protocol.ClientAuthenticator;
import com.jmqtt.router.SubscriptionRegistry;
import com.jmqtt.session.ClientSession;
import com.jmqtt.session.SessionRegistry;
import com.jmqtt.store.RetainedMessage;
import com.jmqtt.store.RetainedMessageStore;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.util.AttributeKey;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class SimpleBrokerMessageHandler implements BrokerMessageHandler, BrokerClusterReceiver {
    private static final Logger LOG = Logger.getLogger(SimpleBrokerMessageHandler.class.getName());
    private static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("jmqtt.clientId");
    private static final AttributeKey<Boolean> CLEAN_SESSION = AttributeKey.valueOf("jmqtt.cleanSession");

    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private final RetainedMessageStore retainedMessageStore;
    private final ClientAuthenticator clientAuthenticator;
    private final AclAuthorizer aclAuthorizer;
    private final ClusterReplicator clusterReplicator;

    public SimpleBrokerMessageHandler(
            SessionRegistry sessionRegistry,
            SubscriptionRegistry subscriptionRegistry,
            RetainedMessageStore retainedMessageStore,
            ClientAuthenticator clientAuthenticator,
            AclAuthorizer aclAuthorizer,
            ClusterReplicator clusterReplicator) {
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.retainedMessageStore = retainedMessageStore;
        this.clientAuthenticator = clientAuthenticator;
        this.aclAuthorizer = aclAuthorizer;
        this.clusterReplicator = clusterReplicator;
    }

    @Override
    public void onMessage(ChannelHandlerContext ctx, MqttMessage message) {
        if (message.decoderResult().isFailure()) {
            LOG.warning(() -> "[PROTO] decode failed, remote=" + ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        MqttMessageType messageType = message.fixedHeader().messageType();
        switch (messageType) {
            case CONNECT -> handleConnect(ctx, (MqttConnectMessage) message);
            case SUBSCRIBE -> handleSubscribe(ctx, (MqttSubscribeMessage) message);
            case UNSUBSCRIBE -> handleUnsubscribe(ctx, (MqttUnsubscribeMessage) message);
            case PUBLISH -> handlePublish(ctx, (MqttPublishMessage) message);
            case PINGREQ -> {
                LOG.fine(() -> "[PING] clientId=" + currentClientId(ctx.channel()));
                ctx.writeAndFlush(new MqttMessage(
                        new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)
                ));
            }
            case DISCONNECT -> {
                LOG.info(() -> "[DISCONNECT] clientId=" + currentClientId(ctx.channel()));
                ctx.close();
            }
            default -> {
            }
        }
    }

    @Override
    public void onDisconnect(Channel channel) {
        String clientId = channel.attr(CLIENT_ID).get();
        if (clientId == null || clientId.isBlank()) {
            return;
        }

        boolean cleanSession = Optional.ofNullable(channel.attr(CLEAN_SESSION).get()).orElse(false);
        sessionRegistry.remove(clientId);
        if (cleanSession) {
            subscriptionRegistry.removeClient(clientId);
        }
        LOG.info(() -> "[SESSION] offline clientId=" + clientId + ", cleanSession=" + cleanSession);
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {
        String clientId = message.payload().clientIdentifier();
        if (clientId == null || clientId.isBlank()) {
            LOG.warning(() -> "[CONNECT] rejected empty clientId, remote=" + ctx.channel().remoteAddress());
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
            return;
        }

        String username = message.payload().userName();
        String password = message.payload().passwordInBytes() == null
                ? null
                : new String(message.payload().passwordInBytes(), StandardCharsets.UTF_8);

        if (!clientAuthenticator.authenticate(clientId, username, password)) {
            LOG.warning(() -> "[CONNECT] auth failed clientId=" + clientId + ", username=" + username);
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
            return;
        }

        boolean cleanSession = message.variableHeader().isCleanSession();
        int keepAliveSeconds = Math.max(message.variableHeader().keepAliveTimeSeconds(), 0);
        String serviceNodeIp = resolveServiceNodeIp(ctx.channel());
        sessionRegistry.register(new ClientSession(
            clientId,
            ctx.channel(),
            cleanSession,
            username,
            serviceNodeIp,
            keepAliveSeconds,
            Instant.now()
        ));
        ctx.channel().attr(CLIENT_ID).set(clientId);
        ctx.channel().attr(CLEAN_SESSION).set(cleanSession);

        ctx.writeAndFlush(MqttMessageBuilders.connAck()
                .sessionPresent(false)
                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .build());
        LOG.info(() -> "[CONNECT] accepted clientId=" + clientId
            + ", username=" + username
            + ", serviceNodeIp=" + serviceNodeIp
            + ", cleanSession=" + cleanSession
            + ", keepAliveSeconds=" + keepAliveSeconds);
    }

    /**
     * handle subscribe message
     *
     * @param ctx     ctx
     * @param message message
     */
    private void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null) {
            ctx.close();
            return;
        }

        List<Integer> grantedQos = new ArrayList<>();
        for (MqttTopicSubscription subscription : message.payload().topicSubscriptions()) {
            String topicFilter = subscription.topicFilter();
            String normalizedFilter = SharedSubscription.normalizeTopicFilter(topicFilter);
            if (allowed(clientId, normalizedFilter, AclAction.SUBSCRIBE)) {
                grantedQos.add(MqttQoS.FAILURE.value());
                LOG.warning(() -> "[ACL] subscribe denied clientId=" + clientId + ", topicFilter=" + topicFilter);
                continue;
            }
            int qos = Math.min(subscription.qualityOfService().value(), 1);
            subscriptionRegistry.subscribe(clientId, topicFilter, qos);
            grantedQos.add(qos);
            replayRetained(ctx.channel(), normalizedFilter);
            LOG.info(() -> "[SUBSCRIBE] clientId=" + clientId + ", topicFilter=" + topicFilter + ", qos=" + qos);
        }

        MqttMessageBuilders.SubAckBuilder subAckBuilder = MqttMessageBuilders.subAck()
                .packetId(message.variableHeader().messageId());

        grantedQos.forEach(qos -> {
            MqttQoS mqttQoS = MqttQoS.valueOf(qos);
            subAckBuilder.addGrantedQos(mqttQoS);
        });
        ctx.writeAndFlush(subAckBuilder.build());
    }

    private void handleUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null) {
            ctx.close();
            return;
        }

        message.payload().topics().forEach(topicFilter -> subscriptionRegistry.unsubscribe(clientId, topicFilter));
        ctx.writeAndFlush(MqttMessageBuilders.unsubAck().packetId(message.variableHeader().messageId()).build());
        LOG.info(() -> "[UNSUBSCRIBE] clientId=" + clientId + ", topics=" + message.payload().topics());
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        byte[] payload = ByteBufUtil.getBytes(message.payload());
        int qos = message.fixedHeader().qosLevel().value();
        String clientId = currentClientId(ctx.channel());

        if (allowed(clientId, topic, AclAction.PUBLISH)) {
            LOG.warning(() -> "[ACL] publish denied clientId=" + clientId + ", topic=" + topic);
            if (qos == 1) {
                ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(message.variableHeader().packetId()).build());
            }
            return;
        }

        if (message.fixedHeader().isRetain()) {
            retainedMessageStore.saveOrRemove(new RetainedMessage(topic, payload, qos, true));
        }

        routeMessage(topic, payload);
        clusterReplicator.replicatePublish(topic, payload, qos, message.fixedHeader().isRetain());
        LOG.info(() -> "[PUBLISH] clientId=" + clientId + ", topic=" + topic
                + ", qos=" + qos + ", retain=" + message.fixedHeader().isRetain() + ", bytes=" + payload.length);

        if (qos == 1) {
            ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(message.variableHeader().packetId()).build());
        }
    }

    private void routeMessage(String topic, byte[] payload) {
        Set<String> subscribers = subscriptionRegistry.findSubscribers(topic);
        LOG.fine(() -> "[ROUTE] topic=" + topic + ", subscribers=" + subscribers.size());
        for (String subscriber : subscribers) {
            Optional<ClientSession> sessionOptional = sessionRegistry.get(subscriber);
            if (sessionOptional.isEmpty()) {
                continue;
            }

            Channel channel = sessionOptional.get().getChannel();
            if (!channel.isActive()) {
                continue;
            }

            channel.writeAndFlush(MqttMessageBuilders.publish()
                    .topicName(topic)
                    .retained(false)
                    .qos(MqttQoS.AT_MOST_ONCE)
                    .payload(Unpooled.wrappedBuffer(payload))
                    .build());
        }
    }

    private void replayRetained(Channel channel, String topicFilter) {
        List<RetainedMessage> retainedMessages = retainedMessageStore.findByTopicFilter(topicFilter);
        for (RetainedMessage retained : retainedMessages) {
            channel.writeAndFlush(MqttMessageBuilders.publish()
                    .topicName(retained.getTopic())
                    .retained(true)
                    .qos(MqttQoS.AT_MOST_ONCE)
                    .payload(Unpooled.wrappedBuffer(retained.getPayload()))
                    .build());
        }
    }

    private void rejectConnection(ChannelHandlerContext ctx, MqttConnectReturnCode returnCode) {
        ctx.writeAndFlush(MqttMessageBuilders.connAck().sessionPresent(false).returnCode(returnCode).build())
                .addListener(future -> ctx.close());
    }

    private String currentClientId(Channel channel) {
        String clientId = channel.attr(CLIENT_ID).get();
        if (clientId == null || clientId.isBlank()) {
            return null;
        }
        return clientId;
    }

    private boolean allowed(String clientId, String topic, AclAction action) {
        String username = clientId == null ? null : sessionRegistry.get(clientId).map(ClientSession::getUsername).orElse(null);
        return !aclAuthorizer.isAllowed(new AclRequest(clientId, username, topic, action));
    }

    private String resolveServiceNodeIp(Channel channel) {
        String preferred = System.getProperty("jmqtt.node.ip");
        if (preferred == null || preferred.isBlank()) {
            preferred = System.getenv("JMQTT_NODE_IP");
        }
        if (preferred != null && !preferred.isBlank()) {
            return preferred.trim();
        }

        SocketAddress localAddress = channel.localAddress();
        if (localAddress instanceof InetSocketAddress inetSocketAddress) {
            InetAddress inetAddress = inetSocketAddress.getAddress();
            if (inetAddress != null && !inetAddress.isAnyLocalAddress()) {
                return inetAddress.getHostAddress();
            }
        }

        try {
            InetAddress localHost = InetAddress.getLocalHost();
            if (localHost instanceof Inet4Address) {
                return localHost.getHostAddress();
            }
            return localHost.getHostAddress();
        } catch (Exception ignored) {
            return "unknown";
        }
    }

    @Override
    public void onClusterPublish(ClusterPublishMessage message) {
        if (message.isRetain()) {
            retainedMessageStore.saveOrRemove(
                new RetainedMessage(message.getTopic(), message.getPayload(), message.getQos(), true)
            );
        }
        routeMessage(message.getTopic(), message.getPayload());
        LOG.info(() -> "[CLUSTER] deliver topic=" + message.getTopic() + ", qos=" + message.getQos()
            + ", sourceNode=" + message.getSourceNodeId());
    }
}
