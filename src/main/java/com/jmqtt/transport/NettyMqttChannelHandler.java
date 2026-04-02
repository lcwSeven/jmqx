/**
 * @author liucaiwen
 * @date 2026/4/2
 */
package com.jmqtt.transport;

import com.jmqtt.broker.BrokerMessageHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttMessage;

public class NettyMqttChannelHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private final BrokerMessageHandler brokerMessageHandler;

    public NettyMqttChannelHandler(BrokerMessageHandler brokerMessageHandler) {
        this.brokerMessageHandler = brokerMessageHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage message) {
        brokerMessageHandler.onMessage(ctx, message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        brokerMessageHandler.onDisconnect(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        brokerMessageHandler.onDisconnect(ctx.channel());
        ctx.close();
    }
}
