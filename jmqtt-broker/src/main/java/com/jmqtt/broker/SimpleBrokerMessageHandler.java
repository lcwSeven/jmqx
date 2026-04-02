package com.jmqtt.broker;

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
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.util.AttributeKey;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author liucaiwen
 */
public class SimpleBrokerMessageHandler implements BrokerMessageHandler {
    private static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("jmqtt.clientId");
    private static final AttributeKey<Boolean> CLEAN_SESSION = AttributeKey.valueOf("jmqtt.cleanSession");

    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private final RetainedMessageStore retainedMessageStore;
    private final ClientAuthenticator clientAuthenticator;

    public SimpleBrokerMessageHandler(
        SessionRegistry sessionRegistry,
        SubscriptionRegistry subscriptionRegistry,
        RetainedMessageStore retainedMessageStore,
        ClientAuthenticator clientAuthenticator
    ) {
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.retainedMessageStore = retainedMessageStore;
        this.clientAuthenticator = clientAuthenticator;
    }

    @Override
    public void onMessage(ChannelHandlerContext ctx, MqttMessage message) {
        if (message.decoderResult().isFailure()) {
            ctx.close();
            return;
        }

        MqttMessageType messageType = message.fixedHeader().messageType();
        switch (messageType) {
            case CONNECT -> handleConnect(ctx, (MqttConnectMessage) message);
            case SUBSCRIBE -> handleSubscribe(ctx, (MqttSubscribeMessage) message);
            case UNSUBSCRIBE -> handleUnsubscribe(ctx, (MqttUnsubscribeMessage) message);
            case PUBLISH -> handlePublish(ctx, (MqttPublishMessage) message);
            case PINGREQ -> ctx.writeAndFlush(MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                null,
                null
            ));
            case DISCONNECT -> ctx.close();
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
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {
        String clientId = message.payload().clientIdentifier();
        if (clientId == null || clientId.isBlank()) {
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
            return;
        }

        String username = message.payload().userName();
        String password = message.payload().passwordInBytes() == null
            ? null
            : new String(message.payload().passwordInBytes(), StandardCharsets.UTF_8);

        if (!clientAuthenticator.authenticate(username, password)) {
            rejectConnection(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
            return;
        }

        boolean cleanSession = message.variableHeader().isCleanSession();
        sessionRegistry.register(new ClientSession(clientId, ctx.channel(), cleanSession, username, Instant.now()));
        ctx.channel().attr(CLIENT_ID).set(clientId);
        ctx.channel().attr(CLEAN_SESSION).set(cleanSession);

        ctx.writeAndFlush(MqttMessageBuilders.connAck()
            .sessionPresent(false)
            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
            .build());
    }

    private void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage message) {
        String clientId = currentClientId(ctx.channel());
        if (clientId == null) {
            ctx.close();
            return;
        }

        List<Integer> grantedQos = new ArrayList<>();
        for (MqttTopicSubscription subscription : message.payload().topicSubscriptions()) {
            int qos = Math.min(subscription.qualityOfService().value(), 1);
            subscriptionRegistry.subscribe(clientId, subscription.topicName(), qos);
            grantedQos.add(qos);
            replayRetained(ctx.channel(), subscription.topicName());
        }

        MqttMessageBuilders.SubAckBuilder subAckBuilder = MqttMessageBuilders.subAck()
            .packetId(message.variableHeader().messageId());
        grantedQos.forEach(subAckBuilder::addGrantedQos);
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
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        byte[] payload = ByteBufUtil.getBytes(message.payload());
        int qos = message.fixedHeader().qosLevel().value();

        if (message.fixedHeader().isRetain()) {
            retainedMessageStore.saveOrRemove(new RetainedMessage(topic, payload, qos, true));
        }

        routeMessage(topic, payload);

        if (qos == 1) {
            ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(message.variableHeader().packetId()).build());
        }
    }

    private void routeMessage(String topic, byte[] payload) {
        Set<String> subscribers = subscriptionRegistry.findSubscribers(topic);
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
}
