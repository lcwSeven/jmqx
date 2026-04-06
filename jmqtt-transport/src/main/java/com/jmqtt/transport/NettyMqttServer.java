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
import io.netty.util.AttributeKey;

import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class NettyMqttServer {
    private static final Logger LOG = Logger.getLogger(NettyMqttServer.class.getName());
    private static final int MAX_MQTT_MESSAGE_SIZE = 256 * 1024;
    private static final AttributeKey<String> CONNECTION_TYPE = AttributeKey.valueOf("jmqtt.connectionType");

    private final BrokerProperties properties;
    private final BrokerMessageHandler brokerMessageHandler;
    private final ConnectionMetrics connectionMetrics;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyMqttServer(
        BrokerProperties properties,
        BrokerMessageHandler brokerMessageHandler,
        ConnectionMetrics connectionMetrics
    ) {
        this.properties = properties;
        this.brokerMessageHandler = brokerMessageHandler;
        this.connectionMetrics = connectionMetrics;
    }

    public synchronized void start() throws InterruptedException {
        if (serverChannel != null && serverChannel.isActive()) {
            return;
        }

        bossGroup = new NioEventLoopGroup(properties.getBossThreads());
        int workerThreads = Math.max(properties.getWorkerThreads(), 0);
        int readerIdleSeconds = Math.max(properties.getReaderIdleSeconds(), 0);
        workerGroup = new NioEventLoopGroup(workerThreads);

        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.attr(CONNECTION_TYPE).set("mqtt");
                    ch.pipeline()
                        .addLast("idle-state", new IdleStateHandler(readerIdleSeconds, 0, 0))
                        .addLast("mqtt-decoder", new MqttDecoder(MAX_MQTT_MESSAGE_SIZE))
                        .addLast("mqtt-encoder", MqttEncoder.INSTANCE)
                        .addLast("mqtt-handler", new NettyMqttChannelHandler(brokerMessageHandler, connectionMetrics));
                }
            });

        serverChannel = bootstrap.bind(properties.getHost(), properties.getPort()).sync().channel();
        LOG.info(() -> "[SERVER] started on " + properties.getHost() + ":" + properties.getPort()
            + ", readerIdleSeconds=" + readerIdleSeconds);
    }

    public synchronized void stop() {
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel = null;
            LOG.info("[SERVER] channel closed");
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
            LOG.info("[SERVER] worker group stopped");
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
            LOG.info("[SERVER] boss group stopped");
        }
    }
}
