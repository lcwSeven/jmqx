package com.jmqx.cluster.netty;

import com.jmqx.cluster.MetadataReplicator;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    // TLV 协议版本。后续新增字段时只增加 tag，不需要修改协议主结构。
    private static final byte VERSION = 1;
    private static final short TAG_SOURCE_NODE_ID = 1;
    private static final short TAG_TOPIC = 2;
    private static final short TAG_PAYLOAD = 3;
    private static final short TAG_PUBLISH_QOS = 4;
    private static final short TAG_INCLUDE_NORMAL = 5;
    private static final short TAG_SHARED_GROUPS = 6;
    // 本地节点 ID
    private final String localNodeId;
    private final String bindHost;
    private final int bindPort;
    private final int requestTimeoutMs;
    private final Map<String, ClusterEndpoint> nodeEndpointMap;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile ClusterMessageConsumer messageConsumer = (topic, payload, publishQos, includeNormal, sharedGroups) -> {
    };
    private NioEventLoopGroup serverBossGroup;
    private NioEventLoopGroup serverWorkerGroup;
    private NioEventLoopGroup clientGroup;
    private Bootstrap clientBootstrap;
    private Channel serverChannel;
    /**
     * 复用到目标节点的长连接，避免每次发送都重复 connect/close。
     */
    private final ConcurrentMap<ClusterEndpoint, Channel> outboundChannels = new ConcurrentHashMap<>();
    /**
     * 端点级连接中的 Future，避免并发发送时对同一端点重复发起 connect。
     */
    private final ConcurrentMap<ClusterEndpoint, ChannelFuture> outboundConnectFutures = new ConcurrentHashMap<>();

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
        this.messageConsumer = messageConsumer == null ? (topic, payload, publishQos, includeNormal, sharedGroups) -> {
        } : messageConsumer;
    }

    /**
     * 向单个目标节点投递跨节点消息。
     *
     * includeNormal / sharedGroups 用于表达“本次远端投递需要覆盖的订阅目标类型”：
     * 1. includeNormal=true：投递远端普通订阅
     * 2. sharedGroups 非空：仅投递指定共享组
     */
    public void dispatch(
        String topic,
        byte[] payload,
        int publishQos,
        String targetNodeId,
        boolean includeNormal,
        Set<String> sharedGroups
    ) {
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
        byte[] frame = encode(new ClusterPublishFrame(
            localNodeId,
            topic,
            payload,
            normalizeQos(publishQos),
            includeNormal,
            normalizeSharedGroups(sharedGroups)
        ));
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
        // 启动发送端共享资源，避免每次 dispatch 重建 EventLoop 导致额外开销。
        clientGroup = new NioEventLoopGroup(1);
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
            clientBootstrap = new Bootstrap();
            clientBootstrap.group(clientGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, requestTimeoutMs)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new ClusterFrameEncoder());
                        }
                    });
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
        outboundConnectFutures.clear();
        outboundChannels.forEach((endpoint, channel) -> {
            if (channel != null) {
                channel.close();
            }
        });
        outboundChannels.clear();
        if (clientGroup != null) {
            clientGroup.shutdownGracefully().syncUninterruptibly();
            clientGroup = null;
        }
        clientBootstrap = null;
        LOG.info("[CLUSTER][MSG] server stopped");
    }

    private void sendOnce(ClusterEndpoint endpoint, byte[] frame) {
        // 端点级长连接复用，优先走已建立连接发送。
        Channel cached = outboundChannels.get(endpoint);
        if (cached != null && cached.isActive()) {
            writeToChannel(endpoint, cached, frame);
            return;
        }
        // 同一端点复用连接中的 Future，避免并发下重复 connect。
        ChannelFuture connectFuture = outboundConnectFutures.compute(endpoint, (key, previousFuture) -> {
            if (previousFuture != null && !previousFuture.isDone()) {
                return previousFuture;
            }
            Bootstrap bootstrap = clientBootstrap;
            if (bootstrap == null || clientGroup == null || !running.get()) {
                return null;
            }
            return bootstrap.connect(endpoint.host(), endpoint.port());
        });
        if (connectFuture == null) {
            return;
        }
        connectFuture.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                Throwable cause = future.cause();
                String error = cause == null ? "unknown" : cause.getMessage();
                LOG.fine(() -> "[CLUSTER][MSG] connect failed endpoint=" + endpoint + ", error=" + error);
                outboundConnectFutures.remove(endpoint, connectFuture);
                return;
            }
            Channel channel = future.channel();
            outboundChannels.put(endpoint, channel);
            outboundConnectFutures.remove(endpoint, connectFuture);
            channel.closeFuture().addListener(closeFuture -> outboundChannels.remove(endpoint, channel));
            writeToChannel(endpoint, channel, frame);
        });
    }

    private void writeToChannel(ClusterEndpoint endpoint, Channel channel, byte[] frame) {
        if (channel == null || !channel.isActive()) {
            outboundChannels.remove(endpoint, channel);
            return;
        }
        channel.writeAndFlush(frame).addListener((ChannelFutureListener) writeFuture -> {
            if (writeFuture.isSuccess()) {
                return;
            }
            Throwable cause = writeFuture.cause();
            String error = cause == null ? "unknown" : cause.getMessage();
            LOG.fine(() -> "[CLUSTER][MSG] send failed endpoint=" + endpoint + ", error=" + error);
            outboundChannels.remove(endpoint, channel);
            channel.close();
        });
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
     * 协议编码（TLV）：
     * magic(2) + version(1) + fieldCount(2) + N * (tag(2) + length(4) + value)
     */
    private static byte[] encode(ClusterPublishFrame frame) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            DataOutputStream data = new DataOutputStream(out);
            data.writeShort(MAGIC);
            data.writeByte(VERSION);
            List<Field> fields = buildFields(frame);
            data.writeShort(fields.size());
            for (Field field : fields) {
                data.writeShort(field.tag());
                data.writeInt(field.value().length);
                data.write(field.value());
            }
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode cluster publish frame failed", exception);
        }
    }

    /**
     * 协议解码（TLV）：
     * 解码时跳过未知 tag，天然支持后续字段扩展。
     */
    private static ClusterPublishFrame decode(byte[] payload) {
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
            short magic = input.readShort();
            if (magic != MAGIC) {
                throw new IllegalStateException("invalid cluster publish frame magic");
            }
            byte version = input.readByte();
            if (version != VERSION) {
                throw new IllegalStateException("unsupported cluster publish frame version: " + version);
            }
            int fieldCount = Short.toUnsignedInt(input.readShort());
            String sourceNodeId = null;
            String topic = null;
            byte[] body = null;
            int publishQos = 0;
            boolean includeNormal = true;
            Set<String> sharedGroups = null;
            for (int i = 0; i < fieldCount; i++) {
                int tag = Short.toUnsignedInt(input.readShort());
                int length = input.readInt();
                if (length < 0) {
                    throw new IllegalStateException("invalid field length: " + length);
                }
                byte[] value = new byte[length];
                input.readFully(value);
                switch (tag) {
                    case TAG_SOURCE_NODE_ID -> sourceNodeId = decodeString(value);
                    case TAG_TOPIC -> topic = decodeString(value);
                    case TAG_PAYLOAD -> body = value;
                    case TAG_PUBLISH_QOS -> publishQos = decodeInt(value);
                    case TAG_INCLUDE_NORMAL -> includeNormal = decodeBoolean(value);
                    case TAG_SHARED_GROUPS -> sharedGroups = decodeSharedGroups(value);
                    default -> {
                        // 未知字段直接忽略，实现向前兼容。
                    }
                }
            }
            if (topic == null || topic.isBlank() || body == null) {
                throw new IllegalStateException("missing required field(topic/payload)");
            }
            return new ClusterPublishFrame(sourceNodeId, topic, body, normalizeQos(publishQos), includeNormal, sharedGroups);
        } catch (Exception exception) {
            throw new IllegalStateException("decode cluster publish frame failed", exception);
        }
    }

    private static int normalizeQos(int qos) {
        if (qos >= 2) {
            return 2;
        }
        return qos == 1 ? 1 : 0;
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

    private static List<Field> buildFields(ClusterPublishFrame frame) {
        List<Field> fields = new ArrayList<>(6);
        if (frame.sourceNodeId() != null && !frame.sourceNodeId().isBlank()) {
            fields.add(new Field(TAG_SOURCE_NODE_ID, encodeString(frame.sourceNodeId())));
        }
        fields.add(new Field(TAG_TOPIC, encodeString(frame.topic())));
        fields.add(new Field(TAG_PAYLOAD, frame.payload() == null ? new byte[0] : frame.payload()));
        fields.add(new Field(TAG_PUBLISH_QOS, encodeInt(normalizeQos(frame.publishQos()))));
        fields.add(new Field(TAG_INCLUDE_NORMAL, encodeBoolean(frame.includeNormal())));
        Set<String> normalizedGroups = normalizeSharedGroups(frame.sharedGroups());
        if (normalizedGroups != null) {
            fields.add(new Field(TAG_SHARED_GROUPS, encodeSharedGroups(normalizedGroups)));
        }
        return fields;
    }

    private static byte[] encodeString(String value) {
        if (value == null) {
            return new byte[0];
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String decodeString(byte[] value) {
        if (value == null || value.length == 0) {
            return null;
        }
        return new String(value, StandardCharsets.UTF_8);
    }

    private static byte[] encodeInt(int value) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(4);
            DataOutputStream data = new DataOutputStream(out);
            data.writeInt(value);
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode int field failed", exception);
        }
    }

    private static int decodeInt(byte[] value) {
        if (value == null || value.length != Integer.BYTES) {
            return 0;
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(value));
            return input.readInt();
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static byte[] encodeBoolean(boolean value) {
        return new byte[]{(byte) (value ? 1 : 0)};
    }

    private static boolean decodeBoolean(byte[] value) {
        return value != null && value.length > 0 && value[0] != 0;
    }

    private static byte[] encodeSharedGroups(Set<String> groups) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(64);
            DataOutputStream data = new DataOutputStream(out);
            data.writeShort(groups.size());
            for (String group : groups) {
                byte[] bytes = encodeString(group);
                data.writeShort(bytes.length);
                data.write(bytes);
            }
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode shared groups failed", exception);
        }
    }

    private static Set<String> decodeSharedGroups(byte[] value) {
        if (value == null || value.length == 0) {
            return Collections.emptySet();
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(value));
            int count = Short.toUnsignedInt(input.readShort());
            Set<String> groups = new LinkedHashSet<>();
            for (int i = 0; i < count; i++) {
                int length = Short.toUnsignedInt(input.readShort());
                byte[] bytes = new byte[length];
                input.readFully(bytes);
                String group = decodeString(bytes);
                if (group != null && !group.isBlank()) {
                    groups.add(group);
                }
            }
            return groups;
        } catch (Exception ignored) {
            return Collections.emptySet();
        }
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
            messageConsumer.accept(msg.topic(), msg.payload(), msg.publishQos(), msg.includeNormal(), msg.sharedGroups());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    @FunctionalInterface
    public interface ClusterMessageConsumer {
        void accept(String topic, byte[] payload, int publishQos, boolean includeNormal, Set<String> sharedGroups);
    }

    private record ClusterPublishFrame(
        String sourceNodeId,
        String topic,
        byte[] payload,
        int publishQos,
        boolean includeNormal,
        Set<String> sharedGroups
    ) {
    }

    private record Field(short tag, byte[] value) {
    }
}
