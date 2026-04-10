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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;

import java.util.logging.Logger;

/**
 * MQTT 接入统一服务实现，支持 MQTT/MQTTS/WS/WSS 四种端点形态。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class NettyMqttEndpointServer {
    private static final Logger LOG = Logger.getLogger(NettyMqttEndpointServer.class.getName());
    private static final int MAX_MQTT_MESSAGE_SIZE = 256 * 1024;
    private static final String WS_SUB_PROTOCOLS = "mqtt,mqttv3.1,mqttv3.1.1";
    private static final AttributeKey<String> CONNECTION_TYPE = AttributeKey.valueOf("jmqx.connectionType");

    private final BrokerProperties properties;
    private final BrokerMessageHandler brokerMessageHandler;
    private final ConnectionMetrics connectionMetrics;
    private final EndpointSpec endpointSpec;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyMqttEndpointServer(
        BrokerProperties properties,
        BrokerMessageHandler brokerMessageHandler,
        ConnectionMetrics connectionMetrics,
        EndpointSpec endpointSpec
    ) {
        this.properties = properties;
        this.brokerMessageHandler = brokerMessageHandler;
        this.connectionMetrics = connectionMetrics;
        this.endpointSpec = endpointSpec;
    }

    public synchronized void start() throws InterruptedException {
        if (!endpointSpec.enabled()) {
            return;
        }
        if (serverChannel != null && serverChannel.isActive()) {
            return;
        }

        SslContext sslContext = null;
        if (endpointSpec.tlsEnabled()) {
            sslContext = TlsSslContextProvider.buildServerSslContext(properties);
        }

        bossGroup = new NioEventLoopGroup(properties.getBossThreads());
        int workerThreads = Math.max(properties.getWorkerThreads(), 0);
        int readerIdleSeconds = Math.max(properties.getReaderIdleSeconds(), 0);
        workerGroup = new NioEventLoopGroup(workerThreads);

        String wsPath = endpointSpec.websocket() ? normalizePath(endpointSpec.websocketPath()) : null;
        SslContext finalSslContext = sslContext;
        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.attr(CONNECTION_TYPE).set(endpointSpec.connectionType());
                    if (finalSslContext != null) {
                        ch.pipeline().addLast("ssl", finalSslContext.newHandler(ch.alloc()));
                    }
                    ch.pipeline().addLast("idle-state", new IdleStateHandler(readerIdleSeconds, 0, 0));

                    if (endpointSpec.websocket()) {
                        ch.pipeline()
                            .addLast("http-codec", new HttpServerCodec())
                            .addLast("http-aggregator", new HttpObjectAggregator(64 * 1024))
                            .addLast("ws-handshake-info", new WebSocketHandshakeInfoHandler())
                            .addLast("ws-protocol", new WebSocketServerProtocolHandler(wsPath, WS_SUB_PROTOCOLS, true))
                            .addLast("ws-frame-decoder", new WebSocketFrameToByteBufDecoder())
                            .addLast("mqtt-decoder", new MqttDecoder(MAX_MQTT_MESSAGE_SIZE))
                            .addLast("ws-frame-encoder", new ByteBufToWebSocketFrameEncoder())
                            .addLast("mqtt-encoder", MqttEncoder.INSTANCE)
                            .addLast("mqtt-handler", new NettyMqttChannelHandler(brokerMessageHandler, connectionMetrics));
                        return;
                    }

                    ch.pipeline()
                        .addLast("mqtt-decoder", new MqttDecoder(MAX_MQTT_MESSAGE_SIZE))
                        .addLast("mqtt-encoder", MqttEncoder.INSTANCE)
                        .addLast("mqtt-handler", new NettyMqttChannelHandler(brokerMessageHandler, connectionMetrics));
                }
            });

        serverChannel = bootstrap.bind(endpointSpec.host(), endpointSpec.port()).sync().channel();
        LOG.info(() -> "[" + endpointSpec.logName() + "] started on " + endpointSpec.host() + ":" + endpointSpec.port()
            + (endpointSpec.websocket() ? ", path=" + wsPath : "")
            + ", readerIdleSeconds=" + readerIdleSeconds);
    }

    public synchronized void stop() {
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel = null;
            LOG.info("[" + endpointSpec.logName() + "] channel closed");
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
            LOG.info("[" + endpointSpec.logName() + "] worker group stopped");
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
            LOG.info("[" + endpointSpec.logName() + "] boss group stopped");
        }
    }

    private String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "/mqtt";
        }
        if (path.startsWith("/")) {
            return path;
        }
        return "/" + path;
    }

    /**
     * 端点描述配置。
     *
     * @author liucaiwen
     * @date 2026/4/7
     */
    public record EndpointSpec(
        boolean enabled,
        String host,
        int port,
        boolean tlsEnabled,
        boolean websocket,
        String websocketPath,
        String connectionType,
        String logName
    ) {
    }
}
