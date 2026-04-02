/**
 * @author liucaiwen
 * @date 2026/4/2
 */
package com.jmqtt.broker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;

public interface BrokerMessageHandler {
    void onMessage(ChannelHandlerContext ctx, MqttMessage message);

    void onDisconnect(Channel channel);
}
