package com.jmqx.cluster.netty;

import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * REPLICANT 侧 Netty 元数据写网关。
 * 支持自动根据 CORE 返回的 leader 地址重试。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class NettyMetadataCommandGateway implements MetadataCommandGateway {
    private static final Logger LOG = Logger.getLogger(NettyMetadataCommandGateway.class.getName());
    private static final long DEFAULT_PREFERRED_LEADER_TTL_MS = 15_000L;
    private static final int DEFAULT_PREFERRED_LEADER_MAX_FAILURES = 3;

    private final List<ClusterEndpoint> endpoints;
    private final int requestTimeoutMs;
    private final long preferredLeaderTtlMs;
    private final int preferredLeaderMaxFailures;
    private final NioEventLoopGroup ioGroup;
    private final ConcurrentMap<ClusterEndpoint, EndpointClient> endpointClients = new ConcurrentHashMap<>();
    private final AtomicLong requestId = new AtomicLong(1L);
    private final AtomicReference<String> preferredLeader = new AtomicReference<>();
    private final AtomicLong preferredLeaderUpdatedAt = new AtomicLong(0L);
    private final AtomicInteger preferredLeaderFailureCount = new AtomicInteger(0);

    public NettyMetadataCommandGateway(Collection<String> endpoints, int requestTimeoutMs) {
        this(endpoints, requestTimeoutMs, DEFAULT_PREFERRED_LEADER_TTL_MS, DEFAULT_PREFERRED_LEADER_MAX_FAILURES);
    }

    public NettyMetadataCommandGateway(
        Collection<String> endpoints,
        int requestTimeoutMs,
        long preferredLeaderTtlMs,
        int preferredLeaderMaxFailures
    ) {
        this.endpoints = parseEndpoints(endpoints);
        this.requestTimeoutMs = Math.max(300, requestTimeoutMs);
        this.preferredLeaderTtlMs = Math.max(1000L, preferredLeaderTtlMs);
        this.preferredLeaderMaxFailures = Math.max(1, preferredLeaderMaxFailures);
        this.ioGroup = new NioEventLoopGroup(1);
    }

    /**
     * 提交元数据命令。
     *
     * @param command 命令
     * @return 提交成功时返回当前已应用索引，否则返回 -1L
     */
    @Override
    public long submit(MetadataCommand command) {
        if (command == null || endpoints.isEmpty()) {
            return -1L;
        }
        String leaderHint = getPreferredLeaderIfValid();
        List<ClusterEndpoint> candidates = buildCandidates(leaderHint);
        for (ClusterEndpoint endpoint : candidates) {
            if (endpoint == null) {
                continue;
            }
            SubmitResponse response = submitToEndpoint(endpoint, command);
            // 这里返回空表示超时，则重试。
            if (response == null) {
                markFailureIfPreferred(endpoint);
                continue;
            }
            // 提交成功时返回当前已应用索引，作为提交确认位置。
            if (response.success()) {
                markPreferredLeader(endpoint.host() + ":" + endpoint.port());
                return response.logIndex();
            }
            // 提交失败时返回 leader 端点，作为重试目标。
            if (response.leaderEndpoint() != null && !response.leaderEndpoint().isBlank()) {
                leaderHint = response.leaderEndpoint();
                markPreferredLeader(leaderHint);
                ClusterEndpoint leaderEndpoint = ClusterEndpoint.parse(leaderHint);
                if (leaderEndpoint != null && !endpoint.equals(leaderEndpoint)) {
                    // 提交到leader节点重试
                    SubmitResponse redirected = submitToEndpoint(leaderEndpoint, command);
                    // 重试成功 标记leader节点
                    if (redirected != null && redirected.success()) {
                        markPreferredLeader(leaderHint);
                        return redirected.logIndex();
                    }
                    markFailureIfPreferred(leaderEndpoint);
                }
                continue;
            }
            markFailureIfPreferred(endpoint);
        }
        return -1L;
    }

    /**
     * 构建候选节点列表。
     *
     * @param preferredLeaderEndpoint 优先 leader 节点
     * @return 候选节点列表
     */
    private List<ClusterEndpoint> buildCandidates(String preferredLeaderEndpoint) {
        List<ClusterEndpoint> result = new ArrayList<>();
        ClusterEndpoint preferred = ClusterEndpoint.parse(preferredLeaderEndpoint);
        if (preferred != null) {
            result.add(preferred);
        }
        for (ClusterEndpoint endpoint : endpoints) {
            if (endpoint.equals(preferred)) {
                continue;
            }
            result.add(endpoint);
        }
        return result;
    }

    /**
     * 提交到指定节点。
     *
     * @param endpoint 节点
     * @param command  命令
     * @return 响应
     */
    private SubmitResponse submitToEndpoint(ClusterEndpoint endpoint, MetadataCommand command) {
        try {
            EndpointClient client = endpointClients.computeIfAbsent(endpoint, this::newEndpointClient);
            long currentRequestId = requestId.getAndIncrement();
            return client.submit(command, currentRequestId, requestTimeoutMs);
        } catch (Exception exception) {
            LOG.warning("[CLUSTER][REPLICANT][NETTY] submit failed endpoint=" + endpoint
                    + ", error=" + exception.getMessage());
            return null;
        }
    }

    private EndpointClient newEndpointClient(ClusterEndpoint endpoint) {
        return new EndpointClient(endpoint);
    }

    /**
     * 获取当前 preferredLeader。
     *
     * @return preferredLeader
     */
    private String getPreferredLeaderIfValid() {
        String leader = preferredLeader.get();
        if (leader == null || leader.isBlank()) {
            return null;
        }
        long updatedAt = preferredLeaderUpdatedAt.get();
        if (updatedAt <= 0L) {
            clearPreferredLeader();
            return null;
        }
        long age = System.currentTimeMillis() - updatedAt;
        if (age > preferredLeaderTtlMs) {
            clearPreferredLeader();
            return null;
        }
        return leader;
    }

    private void markPreferredLeader(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return;
        }
        preferredLeader.set(endpoint);
        preferredLeaderUpdatedAt.set(System.currentTimeMillis());
        preferredLeaderFailureCount.set(0);
    }

    private void markFailureIfPreferred(ClusterEndpoint endpoint) {
        if (endpoint == null) {
            return;
        }
        String current = preferredLeader.get();
        if (current == null || current.isBlank()) {
            return;
        }
        String currentEndpoint = endpoint.host() + ":" + endpoint.port();
        if (!currentEndpoint.equals(current)) {
            return;
        }
        int failures = preferredLeaderFailureCount.incrementAndGet();
        if (failures >= preferredLeaderMaxFailures) {
            clearPreferredLeader();
        }
    }

    /**
     * 清空 preferred leader
     */
    private void clearPreferredLeader() {
        preferredLeader.set(null);
        preferredLeaderUpdatedAt.set(0L);
        preferredLeaderFailureCount.set(0);
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

    private final class EndpointClient {
        private final ClusterEndpoint endpoint;
        private final SubmitExchange exchange = new SubmitExchange();
        private final Bootstrap clientBootstrap;
        private volatile Channel channel;

        private EndpointClient(ClusterEndpoint endpoint) {
            this.endpoint = endpoint;
            this.clientBootstrap = new Bootstrap()
                .group(ioGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                        ch.pipeline().addLast(MetadataWireCodec.decoder());
                        ch.pipeline().addLast(new LengthFieldPrepender(4));
                        ch.pipeline().addLast(MetadataWireCodec.encoder());
                        ch.pipeline().addLast(new SubmitResponseHandler(exchange));
                    }
                });
        }

        private synchronized SubmitResponse submit(MetadataCommand command, long currentRequestId, int timeoutMs) {
            Channel ch = ensureChannel();
            if (ch == null || !ch.isActive()) {
                return null;
            }
            exchange.prepare(currentRequestId);
            ch.writeAndFlush(new MetadataWireMessage(
                MetadataMessageType.SUBMIT_REQUEST,
                currentRequestId,
                command,
                0L,
                0L,
                null,
                true,
                null,
                null
            )).syncUninterruptibly();
            boolean completed;
            try {
                completed = exchange.await(timeoutMs);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                closeChannel();
                return null;
            }
            if (!completed) {
                LOG.warning("[CLUSTER][REPLICANT][NETTY] submit timeout endpoint=" + endpoint);
                closeChannel();
                return null;
            }
            SubmitResponse response = exchange.response();
            if (response == null) {
                closeChannel();
            }
            return response;
        }

        private Channel ensureChannel() {
            Channel current = channel;
            if (current != null && current.isActive()) {
                return current;
            }
            try {
                Channel connected = clientBootstrap.connect(endpoint.host(), endpoint.port()).syncUninterruptibly().channel();
                this.channel = connected;
                return connected;
            } catch (Exception exception) {
                LOG.warning("[CLUSTER][REPLICANT][NETTY] connect failed endpoint=" + endpoint
                    + ", error=" + exception.getMessage());
                this.channel = null;
                return null;
            }
        }

        private synchronized void closeChannel() {
            Channel ch = this.channel;
            this.channel = null;
            if (ch != null) {
                ch.close().syncUninterruptibly();
            }
        }
    }

    private static final class SubmitExchange {
        private volatile SubmitResponse response;
        private volatile long expectedRequestId = -1L;
        private volatile CountDownLatch currentLatch = new CountDownLatch(1);

        private synchronized void prepare(long requestId) {
            this.expectedRequestId = requestId;
            this.response = null;
            this.currentLatch = new CountDownLatch(1);
        }

        private void onResponse(MetadataWireMessage message) {
            if (message.type() != MetadataMessageType.SUBMIT_RESPONSE) {
                return;
            }
            if (message.requestId() != expectedRequestId) {
                return;
            }
            response = new SubmitResponse(
                    message.success(),
                    message.logIndex(),
                    message.leaderEndpoint()
            );
            currentLatch.countDown();
        }

        private void onFailure() {
            currentLatch.countDown();
        }

        private boolean await(int timeoutMs) throws InterruptedException {
            return currentLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        private SubmitResponse response() {
            return response;
        }
    }

    private static final class SubmitResponseHandler extends SimpleChannelInboundHandler<MetadataWireMessage> {
        private final SubmitExchange exchange;

        private SubmitResponseHandler(SubmitExchange exchange) {
            this.exchange = exchange;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MetadataWireMessage message) {
            exchange.onResponse(message);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            exchange.onFailure();
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            exchange.onFailure();
            super.channelInactive(ctx);
        }
    }

    private record SubmitResponse(boolean success,
                                  long logIndex,
                                  String leaderEndpoint) {
    }
}
