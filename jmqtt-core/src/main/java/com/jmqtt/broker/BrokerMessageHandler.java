package com.jmqtt.broker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public interface BrokerMessageHandler {
    /**
     * 处理连接消息
     * @param ctx ctx
     * @param message 消息
     */
    void onMessage(ChannelHandlerContext ctx, MqttMessage message);

    /**
     * 处理断开连接的消息
     * @param channel channel
     */
    void onDisconnect(Channel channel);
}
