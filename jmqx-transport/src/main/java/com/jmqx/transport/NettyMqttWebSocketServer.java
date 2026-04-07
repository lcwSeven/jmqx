package com.jmqx.transport;

import com.jmqx.common.BrokerProperties;
import com.jmqx.protocol.BrokerMessageHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;

import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/6
 */
public class NettyMqttWebSocketServer {
    private static final Logger LOG = Logger.getLogger(NettyMqttWebSocketServer.class.getName());
    private static final int MAX_MQTT_MESSAGE_SIZE = 256 * 1024;
    private static final String WS_SUB_PROTOCOLS = "mqtt,mqttv3.1,mqttv3.1.1";
    private static final AttributeKey<String> CONNECTION_TYPE = AttributeKey.valueOf("jmqx.connectionType");

    private final BrokerProperties properties;
    private final BrokerMessageHandler brokerMessageHandler;
    private final ConnectionMetrics connectionMetrics;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyMqttWebSocketServer(
        BrokerProperties properties,
        BrokerMessageHandler brokerMessageHandler,
        ConnectionMetrics connectionMetrics
    ) {
        this.properties = properties;
        this.brokerMessageHandler = brokerMessageHandler;
        this.connectionMetrics = connectionMetrics;
    }

    public synchronized void start() throws InterruptedException {
        if (!properties.isWebsocketEnabled()) {
            return;
        }
        if (serverChannel != null && serverChannel.isActive()) {
            return;
        }

        bossGroup = new NioEventLoopGroup(properties.getBossThreads());
        int workerThreads = Math.max(properties.getWorkerThreads(), 0);
        int readerIdleSeconds = Math.max(properties.getReaderIdleSeconds(), 0);
        workerGroup = new NioEventLoopGroup(workerThreads);

        String websocketPath = normalizeWebsocketPath(properties.getWebsocketPath());
        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.attr(CONNECTION_TYPE).set("websocket");
                    ch.pipeline()
                        .addLast("idle-state", new IdleStateHandler(readerIdleSeconds, 0, 0))
                        .addLast("http-codec", new HttpServerCodec())
                        .addLast("http-aggregator", new HttpObjectAggregator(64 * 1024))
                        .addLast("ws-handshake-info", new WebSocketHandshakeInfoHandler())
                        .addLast("ws-protocol", new WebSocketServerProtocolHandler(websocketPath, WS_SUB_PROTOCOLS, true))
                        .addLast("ws-frame-decoder", new WebSocketFrameToByteBufDecoder())
                        .addLast("mqtt-decoder", new MqttDecoder(MAX_MQTT_MESSAGE_SIZE))
                        .addLast("ws-frame-encoder", new ByteBufToWebSocketFrameEncoder())
                        .addLast("mqtt-encoder", MqttEncoder.INSTANCE)
                        .addLast("mqtt-handler", new NettyMqttChannelHandler(brokerMessageHandler, connectionMetrics));
                }
            });

        serverChannel = bootstrap
            .bind(properties.getWebsocketHost(), properties.getWebsocketPort())
            .sync()
            .channel();
        LOG.info(() -> "[WS] started on " + properties.getWebsocketHost() + ":" + properties.getWebsocketPort()
            + ", path=" + websocketPath + ", readerIdleSeconds=" + readerIdleSeconds);
    }

    public synchronized void stop() {
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel = null;
            LOG.info("[WS] channel closed");
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
            LOG.info("[WS] worker group stopped");
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
            LOG.info("[WS] boss group stopped");
        }
    }

    private String normalizeWebsocketPath(String path) {
        if (path == null || path.isBlank()) {
            return "/mqtt";
        }
        if (path.startsWith("/")) {
            return path;
        }
        return "/" + path;
    }
}
