package com.jmqx.transport;

import com.jmqx.common.logging.ClientLogContext;
import com.jmqx.protocol.BrokerMessageHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.AttributeKey;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class NettyMqttChannelHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private static final Logger LOG = Logger.getLogger(NettyMqttChannelHandler.class.getName());
    private static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("jmqx.clientId");

    private final BrokerMessageHandler brokerMessageHandler;
    private final ConnectionMetrics connectionMetrics;

    public NettyMqttChannelHandler(BrokerMessageHandler brokerMessageHandler, ConnectionMetrics connectionMetrics) {
        this.brokerMessageHandler = brokerMessageHandler;
        this.connectionMetrics = connectionMetrics;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        connectionMetrics.onConnected();
        LOG.info(() -> "[CONNECT] accepted remote=" + ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage message) {
        try (ClientLogContext.Scope ignored = ClientLogContext.open(resolveClientId(ctx, message))) {
            if (message instanceof MqttPublishMessage publishMessage) {
                connectionMetrics.addInboundBytes(publishMessage.payload().readableBytes());
            }
            LOG.info(() -> "[RECV] remote=" + ctx.channel().remoteAddress() + ", type=" + message.fixedHeader().messageType());
            brokerMessageHandler.onMessage(ctx, message);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        connectionMetrics.onDisconnected();
        try (ClientLogContext.Scope ignored = ClientLogContext.open(resolveClientId(ctx, null))) {
            LOG.info(() -> "[DISCONNECT] remote=" + ctx.channel().remoteAddress());
            brokerMessageHandler.onDisconnect(ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        try (ClientLogContext.Scope ignored = ClientLogContext.open(resolveClientId(ctx, null))) {
            LOG.log(Level.WARNING, "[ERROR] remote=" + ctx.channel().remoteAddress() + ", message=" + cause.getMessage(), cause);
            brokerMessageHandler.onDisconnect(ctx.channel());
            ctx.close();
        }
    }

    private String resolveClientId(ChannelHandlerContext ctx, MqttMessage message) {
        String current = ctx.channel().attr(CLIENT_ID).get();
        if (current != null && !current.isBlank()) {
            return current;
        }
        if (message instanceof MqttConnectMessage connectMessage
                && connectMessage.payload() != null
                && connectMessage.payload().clientIdentifier() != null
                && !connectMessage.payload().clientIdentifier().isBlank()) {
            return connectMessage.payload().clientIdentifier();
        }
        return null;
    }
}
