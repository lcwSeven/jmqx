package com.jmqx.cluster.netty;

import com.jmqx.cluster.MetadataReplicator;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * 集群跨节点 PUBLISH 消息转发通道。
 * 所有节点都启动接收端，发送端按 nodeId 定向投递，完成跨节点消息闭环。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class NettyClusterMessageTransport implements MetadataReplicator {
    private static final Logger LOG = Logger.getLogger(NettyClusterMessageTransport.class.getName());
    private static final short MAGIC = (short) 0x4A4D;
    private static final byte VERSION = 2;
    // 本地节点 ID
    private final String localNodeId;
    private final String bindHost;
    private final int bindPort;
    private final int requestTimeoutMs;
    private final Map<String, ClusterEndpoint> nodeEndpointMap;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile ClusterMessageConsumer messageConsumer = (topic, payload, includeNormal, sharedGroups) -> {
    };
    private NioEventLoopGroup serverBossGroup;
    private NioEventLoopGroup serverWorkerGroup;
    private Channel serverChannel;

    /**
     * @param localNodeId      当前节点 ID，用于避免消息回环回发到自己
     * @param bindHost         集群消息服务监听地址
     * @param bindPort         集群消息服务监听端口
     * @param requestTimeoutMs 发送到目标节点时的连接超时
     * @param nodeEndpoints    节点路由表，格式：nodeId -> host:port
     */
    public NettyClusterMessageTransport(
        String localNodeId,
        String bindHost,
        int bindPort,
        int requestTimeoutMs,
        Map<String, String> nodeEndpoints
    ) {
        this.localNodeId = (localNodeId == null || localNodeId.isBlank()) ? "node-1" : localNodeId;
        this.bindHost = (bindHost == null || bindHost.isBlank()) ? "0.0.0.0" : bindHost;
        this.bindPort = bindPort <= 0 ? 7900 : bindPort;
        this.requestTimeoutMs = Math.max(200, requestTimeoutMs);
        this.nodeEndpointMap = parseNodeEndpoints(nodeEndpoints);
    }

    /**
     * 设置集群消息消费者。
     * 该回调在接收端解码成功后执行，最终由 Broker 进行本地消息分发。
     */
    public void setMessageConsumer(ClusterMessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer == null ? (topic, payload, includeNormal, sharedGroups) -> {
        } : messageConsumer;
    }

    /**
     * 向单个目标节点投递跨节点消息。
     *
     * includeNormal / sharedGroups 用于表达“本次远端投递需要覆盖的订阅目标类型”：
     * 1. includeNormal=true：投递远端普通订阅
     * 2. sharedGroups 非空：仅投递指定共享组
     */
    public void dispatch(String topic, byte[] payload, String targetNodeId, boolean includeNormal, Set<String> sharedGroups) {
        if (!running.get() || topic == null || topic.isBlank() || payload == null || targetNodeId == null || targetNodeId.isBlank()) {
            return;
        }
        // 本地节点不投递
        if (targetNodeId.equals(localNodeId)) {
            return;
        }
        // 从集群节点中获取目标节点地址
        ClusterEndpoint endpoint = nodeEndpointMap.get(targetNodeId);
        if (endpoint == null) {
            LOG.fine(() -> "[CLUSTER][MSG] missing endpoint for nodeId=" + targetNodeId);
            return;
        }
        // 将消息包装成自定义协议
        byte[] frame = encode(new ClusterPublishFrame(localNodeId, topic, payload, includeNormal, normalizeSharedGroups(sharedGroups)));
        // 投递
        sendOnce(endpoint, frame);
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        // 启动接收端：一个 boss 负责 accept，worker 负责读写与编解码。
        serverBossGroup = new NioEventLoopGroup(1);
        serverWorkerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(serverBossGroup, serverWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // 协议帧格式：4 字节长度 + 业务帧；先拆包，再做业务解码。
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                        ch.pipeline().addLast(new ClusterFrameDecoder());
                        // 发送方向：先加长度头，再写业务帧。
                        ch.pipeline().addLast(new LengthFieldPrepender(4));
                        ch.pipeline().addLast(new ClusterFrameEncoder());
                        ch.pipeline().addLast(new ClusterServerHandler());
                    }
                });
            serverChannel = bootstrap.bind(bindHost, bindPort).syncUninterruptibly().channel();
            LOG.info(() -> "[CLUSTER][MSG] server started nodeId=" + localNodeId + ", bind=" + bindHost + ":" + bindPort);
        } catch (Exception exception) {
            running.set(false);
            stop();
            throw new IllegalStateException("failed to start netty cluster message transport", exception);
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // 停机顺序：先关监听通道，再关 worker，再关 boss，避免出现新连接进入。
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        if (serverWorkerGroup != null) {
            serverWorkerGroup.shutdownGracefully().syncUninterruptibly();
            serverWorkerGroup = null;
        }
        if (serverBossGroup != null) {
            serverBossGroup.shutdownGracefully().syncUninterruptibly();
            serverBossGroup = null;
        }
        LOG.info("[CLUSTER][MSG] server stopped");
    }

    private void sendOnce(ClusterEndpoint endpoint, byte[] frame) {
        // 当前实现是“短连接单次发送”模型：每次 dispatch 临时建连、发送、关闭。
        // 优点：实现简单；缺点：高频跨节点场景下连接开销较大，后续可演进连接池。
        NioEventLoopGroup clientGroup = new NioEventLoopGroup(1);
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(clientGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, requestTimeoutMs)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new LengthFieldPrepender(4));
                        ch.pipeline().addLast(new ClusterFrameEncoder());
                    }
                });
            Channel channel = bootstrap.connect(endpoint.host(), endpoint.port()).syncUninterruptibly().channel();
            channel.writeAndFlush(frame).syncUninterruptibly();
            channel.close().syncUninterruptibly();
        } catch (Exception exception) {
            LOG.fine(() -> "[CLUSTER][MSG] dispatch failed endpoint=" + endpoint + ", error=" + exception.getMessage());
        } finally {
            clientGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    /**
     * 解析静态节点路由表（nodeId -> host:port）。
     */
    private static Map<String, ClusterEndpoint> parseNodeEndpoints(Map<String, String> nodeEndpoints) {
        if (nodeEndpoints == null || nodeEndpoints.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, ClusterEndpoint> result = new HashMap<>();
        nodeEndpoints.forEach((nodeId, rawEndpoint) -> {
            if (nodeId == null || nodeId.isBlank()) {
                return;
            }
            ClusterEndpoint endpoint = ClusterEndpoint.parse(rawEndpoint);
            if (endpoint != null) {
                result.put(nodeId.trim(), endpoint);
            }
        });
        return result;
    }

    /**
     * 协议编码：
     * magic(2) + version(1) + sourceNodeId + topic + payload + includeNormal + sharedGroups
     */
    private static byte[] encode(ClusterPublishFrame frame) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            DataOutputStream data = new DataOutputStream(out);
            data.writeShort(MAGIC);
            data.writeByte(VERSION);
            writeNullable(data, frame.sourceNodeId());
            writeNullable(data, frame.topic());
            data.writeInt(frame.payload().length);
            data.write(frame.payload());
            data.writeBoolean(frame.includeNormal());
            Set<String> sharedGroups = frame.sharedGroups();
            if (sharedGroups == null) {
                data.writeInt(-1);
            } else {
                data.writeInt(sharedGroups.size());
                for (String group : sharedGroups) {
                    data.writeUTF(group);
                }
            }
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode cluster publish frame failed", exception);
        }
    }

    /**
     * 协议解码，同时兼容历史 V1 帧：
     * V1 不包含 includeNormal/sharedGroups，默认等价 includeNormal=true。
     */
    private static ClusterPublishFrame decode(byte[] payload) {
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
            short magic = input.readShort();
            if (magic != MAGIC) {
                throw new IllegalStateException("invalid cluster publish frame magic");
            }
            byte version = input.readByte();
            String sourceNodeId = readNullable(input);
            String topic = readNullable(input);
            int bodyLength = input.readInt();
            if (bodyLength < 0) {
                throw new IllegalStateException("invalid cluster publish payload length");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            if (version == 1) {
                return new ClusterPublishFrame(sourceNodeId, topic, body, true, null);
            }
            if (version != VERSION) {
                throw new IllegalStateException("unsupported cluster publish frame version: " + version);
            }
            boolean includeNormal = input.readBoolean();
            int groupCount = input.readInt();
            Set<String> sharedGroups = null;
            if (groupCount >= 0) {
                sharedGroups = new LinkedHashSet<>();
                for (int i = 0; i < groupCount; i++) {
                    sharedGroups.add(input.readUTF());
                }
            }
            return new ClusterPublishFrame(sourceNodeId, topic, body, includeNormal, sharedGroups);
        } catch (Exception exception) {
            throw new IllegalStateException("decode cluster publish frame failed", exception);
        }
    }

    /**
     * 共享组集合归一化：
     * 1. 过滤空值
     * 2. 按字典序排序，保证编码结果稳定（便于排查与测试）
     */
    private static Set<String> normalizeSharedGroups(Set<String> sharedGroups) {
        if (sharedGroups == null) {
            return null;
        }
        List<String> sorted = new ArrayList<>();
        for (String group : sharedGroups) {
            if (group == null || group.isBlank()) {
                continue;
            }
            sorted.add(group);
        }
        Collections.sort(sorted);
        return new LinkedHashSet<>(sorted);
    }

    private static void writeNullable(DataOutputStream out, String value) throws Exception {
        if (value == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeUTF(value);
    }

    private static String readNullable(DataInputStream input) throws Exception {
        if (!input.readBoolean()) {
            return null;
        }
        return input.readUTF();
    }

    private static final class ClusterFrameEncoder extends io.netty.handler.codec.MessageToByteEncoder<byte[]> {
        @Override
        protected void encode(ChannelHandlerContext ctx, byte[] msg, ByteBuf out) {
            out.writeBytes(msg);
        }
    }

    private static final class ClusterFrameDecoder extends io.netty.handler.codec.ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, java.util.List<Object> out) {
            if (!in.isReadable()) {
                return;
            }
            byte[] payload = new byte[in.readableBytes()];
            in.readBytes(payload);
            out.add(NettyClusterMessageTransport.decode(payload));
        }
    }

    private final class ClusterServerHandler extends SimpleChannelInboundHandler<ClusterPublishFrame> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ClusterPublishFrame msg) {
            if (msg == null || msg.topic() == null || msg.topic().isBlank() || msg.payload() == null) {
                return;
            }
            // 将解码后的路由目标约束透传给上层 Broker，交由本地路由器做最终派发。
            messageConsumer.accept(msg.topic(), msg.payload(), msg.includeNormal(), msg.sharedGroups());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    @FunctionalInterface
    public interface ClusterMessageConsumer {
        void accept(String topic, byte[] payload, boolean includeNormal, Set<String> sharedGroups);
    }

    private record ClusterPublishFrame(
        String sourceNodeId,
        String topic,
        byte[] payload,
        boolean includeNormal,
        Set<String> sharedGroups
    ) {
    }
}
