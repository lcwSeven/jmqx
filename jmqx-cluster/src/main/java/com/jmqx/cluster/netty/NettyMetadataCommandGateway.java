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
import java.util.concurrent.TimeUnit;
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

    private final List<ClusterEndpoint> endpoints;
    private final int requestTimeoutMs;
    private final AtomicLong requestId = new AtomicLong(1L);
    private final AtomicReference<String> preferredLeader = new AtomicReference<>();

    public NettyMetadataCommandGateway(Collection<String> endpoints, int requestTimeoutMs) {
        this.endpoints = parseEndpoints(endpoints);
        this.requestTimeoutMs = Math.max(300, requestTimeoutMs);
    }

    @Override
    public long submit(MetadataCommand command) {
        if (command == null || endpoints.isEmpty()) {
            return -1L;
        }
        String leaderHint = preferredLeader.get();
        List<ClusterEndpoint> candidates = buildCandidates(leaderHint);
        for (ClusterEndpoint endpoint : candidates) {
            if (endpoint == null) {
                continue;
            }
            SubmitResponse response = submitToEndpoint(endpoint, command);
            if (response == null) {
                continue;
            }
            if (response.success()) {
                preferredLeader.set(endpoint.host() + ":" + endpoint.port());
                return response.logIndex();
            }
            if (response.leaderEndpoint() != null && !response.leaderEndpoint().isBlank()) {
                leaderHint = response.leaderEndpoint();
                preferredLeader.set(leaderHint);
                ClusterEndpoint leaderEndpoint = ClusterEndpoint.parse(leaderHint);
                if (leaderEndpoint != null && !endpoint.equals(leaderEndpoint)) {
                    SubmitResponse redirected = submitToEndpoint(leaderEndpoint, command);
                    if (redirected != null && redirected.success()) {
                        preferredLeader.set(leaderHint);
                        return redirected.logIndex();
                    }
                }
                continue;
            }
        }
        return -1L;
    }

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

    private SubmitResponse submitToEndpoint(ClusterEndpoint endpoint, MetadataCommand command) {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        SubmitResponseHandler handler = new SubmitResponseHandler();
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
                        ch.pipeline().addLast(handler);
                    }
                });
            Channel channel = bootstrap.connect(endpoint.host(), endpoint.port()).syncUninterruptibly().channel();
            long currentRequestId = requestId.getAndIncrement();
            channel.writeAndFlush(new MetadataWireMessage(
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
            boolean completed = handler.await(requestTimeoutMs);
            channel.close().syncUninterruptibly();
            if (!completed) {
                LOG.warning("[CLUSTER][REPLICANT][NETTY] submit timeout endpoint=" + endpoint);
                return null;
            }
            return handler.response();
        } catch (Exception exception) {
            LOG.warning("[CLUSTER][REPLICANT][NETTY] submit failed endpoint=" + endpoint
                + ", error=" + exception.getMessage());
            return null;
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
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

    private static final class SubmitResponseHandler extends SimpleChannelInboundHandler<MetadataWireMessage> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile SubmitResponse response;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MetadataWireMessage message) {
            if (message.type() != MetadataMessageType.SUBMIT_RESPONSE) {
                return;
            }
            response = new SubmitResponse(
                message.success(),
                message.logIndex(),
                message.leaderEndpoint()
            );
            latch.countDown();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            latch.countDown();
            ctx.close();
        }

        private boolean await(int timeoutMs) throws InterruptedException {
            return latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        private SubmitResponse response() {
            return response;
        }
    }

    private record SubmitResponse(boolean success, long logIndex, String leaderEndpoint) {
    }
}
