/**
 * @author liucaiwen
 * @date 2026/4/2
 */
package com.jmqtt.transport;

import com.jmqtt.broker.BrokerMessageHandler;
import com.jmqtt.common.BrokerProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;

public class NettyMqttServer {
    private final BrokerProperties properties;
    private final BrokerMessageHandler brokerMessageHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyMqttServer(BrokerProperties properties, BrokerMessageHandler brokerMessageHandler) {
        this.properties = properties;
        this.brokerMessageHandler = brokerMessageHandler;
    }

    public synchronized void start() throws InterruptedException {
        if (serverChannel != null && serverChannel.isActive()) {
            return;
        }

        bossGroup = new NioEventLoopGroup(properties.getBossThreads());
        workerGroup = properties.getWorkerThreads() > 0
            ? new NioEventLoopGroup(properties.getWorkerThreads())
            : new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast("idle-state", new IdleStateHandler(120, 0, 0))
                        .addLast("mqtt-decoder", new MqttDecoder())
                        .addLast("mqtt-encoder", MqttEncoder.INSTANCE)
                        .addLast("mqtt-handler", new NettyMqttChannelHandler(brokerMessageHandler));
                }
            });

        serverChannel = bootstrap.bind(properties.getHost(), properties.getPort()).sync().channel();
    }

    public synchronized void stop() {
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
    }
}
