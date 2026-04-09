package com.jmqx.cluster.netty;

import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataLogApplier;
import com.jmqx.cluster.MetadataReplicator;
import com.jmqx.cluster.netty.protocol.MetadataMessageType;
import com.jmqx.cluster.netty.protocol.MetadataWireCodec;
import com.jmqx.cluster.netty.protocol.MetadataWireMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * REPLICANT 侧 Netty 元数据同步客户端。
 * 通过长连接订阅 CORE 已提交日志，按顺序应用并上报 ACK。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class NettyMetadataReplicantSyncClient implements MetadataReplicator {
    private static final Logger LOG = Logger.getLogger(NettyMetadataReplicantSyncClient.class.getName());

    private final String replicantNodeId;
    private final List<ClusterEndpoint> endpoints;
    private final int reconnectBackoffMs;
    private final int ackBatchSize;
    private final int ackFlushIntervalMs;
    private final MetadataLogApplier applier;
    private final Runnable resetAction;
    private final AtomicLong lastAppliedLogIndex = new AtomicLong(0L);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<ClusterEndpoint, EndpointHealth> endpointHealth = new HashMap<>();

    private Thread worker;
    private NioEventLoopGroup group;
    private volatile Channel channel;

    public NettyMetadataReplicantSyncClient(
        String replicantNodeId,
        Collection<String> endpoints,
        int reconnectBackoffMs,
        int ackBatchSize,
        int ackFlushIntervalMs,
        MetadataLogApplier applier,
        Runnable resetAction
    ) {
        this.replicantNodeId = replicantNodeId;
        this.endpoints = parseEndpoints(endpoints);
        this.reconnectBackoffMs = Math.max(300, reconnectBackoffMs);
        this.ackBatchSize = Math.max(1, ackBatchSize);
        this.ackFlushIntervalMs = Math.max(100, ackFlushIntervalMs);
        this.applier = applier;
        this.resetAction = resetAction;
        this.endpoints.forEach(endpoint -> endpointHealth.put(endpoint, new EndpointHealth()));
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        worker = new Thread(this::runLoop, "jmqx-cluster-replicant-netty-sync");
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (worker != null) {
            worker.interrupt();
        }
        Channel current = channel;
        if (current != null) {
            current.close();
        }
        NioEventLoopGroup currentGroup = group;
        if (currentGroup != null) {
            currentGroup.shutdownGracefully();
        }
    }

    private void runLoop() {
        while (running.get()) {
            boolean connected = false;
            for (ClusterEndpoint endpoint : selectEndpoints()) {
                if (!running.get()) {
                    return;
                }
                connected = connectAndConsume(endpoint);
                if (connected) {
                    endpointHealth.computeIfAbsent(endpoint, ignored -> new EndpointHealth()).onSuccess();
                    break;
                }
                endpointHealth.computeIfAbsent(endpoint, ignored -> new EndpointHealth()).onFailure(reconnectBackoffMs);
            }
            sleepQuietly(reconnectBackoffMs);
        }
    }

    private boolean connectAndConsume(ClusterEndpoint endpoint) {
        group = new NioEventLoopGroup(1);
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                        ch.pipeline().addLast(MetadataWireCodec.decoder());
                        ch.pipeline().addLast(new LengthFieldPrepender(4));
                        ch.pipeline().addLast(MetadataWireCodec.encoder());
                        ch.pipeline().addLast(new ReplicantHandler(ackBatchSize, ackFlushIntervalMs));
                    }
                });
            channel = bootstrap.connect(endpoint.host(), endpoint.port()).syncUninterruptibly().channel();
            channel.writeAndFlush(new MetadataWireMessage(
                MetadataMessageType.SUBSCRIBE_REQUEST,
                0L,
                null,
                0L,
                lastAppliedLogIndex.get(),
                replicantNodeId,
                true,
                null,
                null
            )).syncUninterruptibly();
            channel.closeFuture().syncUninterruptibly();
            return running.get();
        } catch (Exception exception) {
            LOG.warning("[CLUSTER][REPLICANT][NETTY] sync failed endpoint=" + endpoint
                + ", error=" + exception.getMessage());
            return false;
        } finally {
            channel = null;
            if (group != null) {
                group.shutdownGracefully().syncUninterruptibly();
                group = null;
            }
        }
    }

    private List<ClusterEndpoint> selectEndpoints() {
        List<ClusterEndpoint> active = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (ClusterEndpoint endpoint : endpoints) {
            EndpointHealth health = endpointHealth.get(endpoint);
            if (health == null || health.availableAtMillis <= now) {
                active.add(endpoint);
            }
        }
        if (!active.isEmpty()) {
            return active;
        }
        return new ArrayList<>(endpoints);
    }

    private final class ReplicantHandler extends SimpleChannelInboundHandler<MetadataWireMessage> {
        private final int ackBatchSize;
        private final int ackFlushIntervalMs;
        private int pendingAckCount;
        private long pendingAckIndex;
        private long lastAckNanos;

        private ReplicantHandler(int ackBatchSize, int ackFlushIntervalMs) {
            this.ackBatchSize = ackBatchSize;
            this.ackFlushIntervalMs = ackFlushIntervalMs;
            this.lastAckNanos = System.nanoTime();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ctx.executor().scheduleAtFixedRate(
                () -> flushAckIfNeeded(ctx, true),
                ackFlushIntervalMs,
                ackFlushIntervalMs,
                TimeUnit.MILLISECONDS
            );
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MetadataWireMessage message) {
            if (message.type() == MetadataMessageType.RESET) {
                if (resetAction != null) {
                    resetAction.run();
                }
                lastAppliedLogIndex.set(0L);
                pendingAckCount = 0;
                pendingAckIndex = 0L;
                return;
            }
            if (message.type() != MetadataMessageType.EVENT) {
                return;
            }
            MetadataCommand command = message.command();
            long logIndex = message.logIndex();
            if (applier != null) {
                applier.apply(logIndex, command);
            }
            long applied = lastAppliedLogIndex.updateAndGet(current -> Math.max(current, logIndex));
            pendingAckIndex = Math.max(pendingAckIndex, applied);
            pendingAckCount++;
            flushAckIfNeeded(ctx, false);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            flushAckIfNeeded(ctx, true);
        }

        private void flushAckIfNeeded(ChannelHandlerContext ctx, boolean force) {
            if (!ctx.channel().isActive()) {
                return;
            }
            if (pendingAckCount <= 0) {
                return;
            }
            long now = System.nanoTime();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(now - lastAckNanos);
            if (!force && pendingAckCount < ackBatchSize && elapsedMs < ackFlushIntervalMs) {
                return;
            }
            ctx.writeAndFlush(new MetadataWireMessage(
                MetadataMessageType.ACK_REQUEST,
                0L,
                null,
                0L,
                pendingAckIndex,
                replicantNodeId,
                true,
                null,
                null
            ));
            pendingAckCount = 0;
            lastAckNanos = now;
        }
    }

    private static List<ClusterEndpoint> parseEndpoints(Collection<String> values) {
        List<ClusterEndpoint> result = new ArrayList<>();
        if (values == null) {
            return result;
        }
        for (String value : values) {
            ClusterEndpoint endpoint = ClusterEndpoint.parse(value);
            if (endpoint != null) {
                result.add(endpoint);
            }
        }
        return result;
    }

    private static void sleepQuietly(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Core 节点可用性状态。
     * 失败后短暂熔断，避免持续打到异常节点。
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private static final class EndpointHealth {
        private int failureCount;
        private long availableAtMillis;

        private void onSuccess() {
            failureCount = 0;
            availableAtMillis = 0L;
        }

        private void onFailure(int baseBackoffMs) {
            failureCount = Math.min(failureCount + 1, 10);
            int factor = 1 << Math.min(failureCount, 6);
            long delay = (long) baseBackoffMs * factor;
            availableAtMillis = System.currentTimeMillis() + Math.min(delay, 30_000L);
        }
    }
}
