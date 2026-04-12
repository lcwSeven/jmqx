package com.jmqx.cluster.netty;

import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;
import com.jmqx.cluster.MetadataLogApplier;
import com.jmqx.cluster.MetadataReplicator;
import com.jmqx.cluster.core.MetadataSnapshot;
import com.jmqx.cluster.core.SofaJraftMetadataCommandGateway;
import com.jmqx.cluster.netty.protocol.MetadataMessageType;
import com.jmqx.cluster.netty.protocol.MetadataWireCodec;
import com.jmqx.cluster.netty.protocol.MetadataWireMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * CORE 侧元数据 Netty 服务端。
 * 提供命令提交、订阅事件流、ACK 水位上报、重放和快照重置能力。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class NettyMetadataCoreServer implements MetadataReplicator, MetadataLogApplier {
    private static final Logger LOG = Logger.getLogger(NettyMetadataCoreServer.class.getName());

    private final String bindHost;
    private final int bindPort;
    private final MetadataCommandGateway gateway;
    private final Supplier<MetadataSnapshot> snapshotSupplier;
    private final int maxReplayEvents;
    /**
     * 单个 replicant 允许的最大未 ACK 事件数窗口。
     * 超过窗口后暂停继续推送，等待 ACK 回补后再发送。
     */
    private final int replicantMaxInFlightEvents;
    /**
     * 每次触发推送时的最大批量条数，避免单次循环占用过久。
     */
    private final int replicantPushBatchSize;
    private final long nodeDownCleanupDelayMs;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentSkipListMap<Long, MetadataCommand> eventHistory = new ConcurrentSkipListMap<>();
    private final AtomicLong lastAppliedLogIndex = new AtomicLong(0L);
    private final Map<String, AtomicLong> replicantAckLogIndex = new ConcurrentHashMap<>();
    private final Map<String, ReplicantSession> replicantSessions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ScheduledFuture<?>> pendingNodeCleanupTasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "jmqx-node-cleanup");
        thread.setDaemon(true);
        return thread;
    });

    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyMetadataCoreServer(
        String bindHost,
        int bindPort,
        MetadataCommandGateway gateway,
        Supplier<MetadataSnapshot> snapshotSupplier,
        int maxReplayEvents,
        int replicantMaxInFlightEvents,
        int replicantPushBatchSize,
        int nodeDownCleanupDelayMs
    ) {
        this.bindHost = (bindHost == null || bindHost.isBlank()) ? "0.0.0.0" : bindHost;
        this.bindPort = bindPort <= 0 ? 7800 : bindPort;
        this.gateway = gateway;
        this.snapshotSupplier = snapshotSupplier == null ? () -> new MetadataSnapshot(0L, List.of()) : snapshotSupplier;
        this.maxReplayEvents = Math.max(1000, maxReplayEvents);
        this.replicantMaxInFlightEvents = Math.max(128, replicantMaxInFlightEvents);
        this.replicantPushBatchSize = Math.max(16, replicantPushBatchSize);
        this.nodeDownCleanupDelayMs = Math.max(1000L, nodeDownCleanupDelayMs);
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                        ch.pipeline().addLast(MetadataWireCodec.decoder());
                        ch.pipeline().addLast(new LengthFieldPrepender(4));
                        ch.pipeline().addLast(MetadataWireCodec.encoder());
                        ch.pipeline().addLast(new CoreHandler());
                    }
                });
            serverChannel = bootstrap.bind(bindHost, bindPort).syncUninterruptibly().channel();
            LOG.info(() -> "[CLUSTER][CORE][NETTY] metadata server started at " + bindHost + ":" + bindPort);
        } catch (Exception exception) {
            running.set(false);
            stop();
            throw new IllegalStateException("failed to start netty metadata core server", exception);
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        replicantSessions.values().forEach(session -> session.ctx().close());
        replicantSessions.clear();
        pendingNodeCleanupTasks.values().forEach(task -> task.cancel(false));
        pendingNodeCleanupTasks.clear();
        cleanupExecutor.shutdownNow();
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
            bossGroup = null;
        }
        LOG.info("[CLUSTER][CORE][NETTY] metadata server stopped");
    }

    @Override
    public void apply(long logIndex, MetadataCommand command) {
        if (!running.get() || command == null) {
            return;
        }
        lastAppliedLogIndex.updateAndGet(current -> Math.max(current, logIndex));
        eventHistory.put(logIndex, command);
        trimHistoryIfNeeded();
        replicantSessions.forEach(this::trySendAvailable);
    }

    private void onSubmit(ChannelHandlerContext ctx, MetadataWireMessage request) {
        MetadataCommand command = request.command();
        long logIndex = gateway.submit(command);
        boolean success = logIndex >= 0;
        String leaderEndpoint = null;
        String errorMessage = null;
        if (!success && gateway instanceof SofaJraftMetadataCommandGateway raftGateway) {
            leaderEndpoint = raftGateway.leaderEndpoint();
            errorMessage = "not leader";
        }
        ctx.writeAndFlush(new MetadataWireMessage(
            MetadataMessageType.SUBMIT_RESPONSE,
            request.requestId(),
            null,
            logIndex,
            0L,
            null,
            success,
            leaderEndpoint,
            errorMessage
        ));
    }

    private void onSubscribe(ChannelHandlerContext ctx, MetadataWireMessage request) {
        String replicantNodeId = request.nodeId();
        if (replicantNodeId == null || replicantNodeId.isBlank()) {
            ctx.close();
            return;
        }
        long knownAck = replicantAckLogIndex.computeIfAbsent(replicantNodeId, ignored -> new AtomicLong(0L)).get();
        long catchupFrom = Math.max(knownAck, Math.max(request.lastAppliedLogIndex(), 0L));
        ReplicantSession newSession = new ReplicantSession(replicantNodeId, ctx, catchupFrom, catchupFrom);
        ReplicantSession oldSession = replicantSessions.put(replicantNodeId, newSession);
        cancelNodeCleanup(replicantNodeId);
        if (oldSession != null && oldSession.ctx() != ctx) {
            oldSession.ctx().close();
        }
        catchupReplicant(newSession, catchupFrom);
    }

    private void scheduleNodeCleanup(String nodeId) {
        if (!running.get() || nodeId == null || nodeId.isBlank()) {
            return;
        }
        cancelNodeCleanup(nodeId);
        ScheduledFuture<?> task = cleanupExecutor.schedule(
            () -> executeNodeCleanup(nodeId),
            nodeDownCleanupDelayMs,
            TimeUnit.MILLISECONDS
        );
        pendingNodeCleanupTasks.put(nodeId, task);
    }

    private void cancelNodeCleanup(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return;
        }
        ScheduledFuture<?> task = pendingNodeCleanupTasks.remove(nodeId);
        if (task != null) {
            task.cancel(false);
        }
    }

    private void executeNodeCleanup(String nodeId) {
        if (!running.get() || nodeId == null || nodeId.isBlank()) {
            pendingNodeCleanupTasks.remove(nodeId);
            return;
        }
        if (replicantSessions.containsKey(nodeId)) {
            pendingNodeCleanupTasks.remove(nodeId);
            return;
        }
        MetadataSnapshot snapshot = snapshotSupplier.get();
        if (snapshot == null || snapshot.commands() == null || snapshot.commands().isEmpty()) {
            pendingNodeCleanupTasks.remove(nodeId);
            return;
        }
        int failed = 0;
        for (MetadataCommand command : snapshot.commands()) {
            if (command == null || !"route.subscription".equals(command.namespace())) {
                continue;
            }
            if (!"register".equals(command.operation())) {
                continue;
            }
            if (!nodeId.equals(command.sourceNodeId())) {
                continue;
            }
            if (command.key() == null || command.key().isBlank()) {
                continue;
            }
            long committed = gateway.submit(new MetadataCommand(
                "route.subscription",
                "unregister",
                command.key(),
                command.value(),
                nodeId
            ));
            if (committed < 0) {
                failed++;
            }
        }
        if (failed > 0 && running.get() && !replicantSessions.containsKey(nodeId)) {
            scheduleNodeCleanup(nodeId);
            return;
        }
        pendingNodeCleanupTasks.remove(nodeId);
    }

    private void onAck(ChannelHandlerContext ctx, MetadataWireMessage request) {
        String replicantNodeId = request.nodeId();
        long ack = request.lastAppliedLogIndex();
        if (replicantNodeId != null && !replicantNodeId.isBlank()) {
            replicantAckLogIndex.computeIfAbsent(replicantNodeId, ignored -> new AtomicLong(0L))
                .updateAndGet(current -> Math.max(current, ack));
            ReplicantSession session = replicantSessions.get(replicantNodeId);
            if (session != null) {
                session.ackedLogIndex().updateAndGet(current -> Math.max(current, ack));
                trySendAvailable(replicantNodeId, session);
            }
        }
        ctx.writeAndFlush(new MetadataWireMessage(
            MetadataMessageType.ACK_RESPONSE,
            request.requestId(),
            null,
            0L,
            0L,
            null,
            true,
            null,
            null
        ));
    }

    private void catchupReplicant(ReplicantSession session, long lastAppliedIndex) {
        ChannelHandlerContext ctx = session.ctx();
        if (isReplayAvailableFrom(lastAppliedIndex)) {
            // 重放命中历史缓冲时走窗口化推送，避免一次性灌满慢节点。
            trySendAvailable(session.nodeId(), session);
            return;
        }
        MetadataSnapshot snapshot = snapshotSupplier.get();
        long snapshotBase = Math.max(snapshot.baseLogIndex(), 0L);
        ctx.writeAndFlush(new MetadataWireMessage(
            MetadataMessageType.RESET,
            0L,
            null,
            snapshotBase,
            0L,
            null,
            true,
            null,
            null
        ));
        int commandCount = snapshot.commands().size();
        long current = commandCount == 0 ? snapshotBase : Math.max(1L, snapshotBase - commandCount + 1L);
        for (MetadataCommand command : snapshot.commands()) {
            ctx.write(new MetadataWireMessage(
                MetadataMessageType.EVENT,
                0L,
                command,
                current,
                0L,
                null,
                true,
                null,
                null
            ));
            current++;
        }
        ctx.flush();
        session.sentLogIndex().updateAndGet(previous -> Math.max(previous, snapshotBase));
        trySendAvailable(session.nodeId(), session);
    }

    /**
     * 判断指定索引后是否仍可直接通过历史事件缓存重放。
     * 返回 false 代表断档过大，需要下发快照重置。
     */
    private boolean isReplayAvailableFrom(long lastAppliedIndex) {
        if (eventHistory.isEmpty()) {
            return true;
        }
        long safeLast = Math.max(lastAppliedIndex, 0L);
        Long oldest = eventHistory.firstKey();
        if (oldest == null) {
            return true;
        }
        return safeLast == 0L || safeLast >= oldest - 1L;
    }

    /**
     * 按窗口推进向 replicant 的增量事件发送。
     * 窗口受两个参数约束：
     * 1. 最大未 ACK 事件数（in-flight）
     * 2. 单次推送批量上限
     */
    private void trySendAvailable(String nodeId, ReplicantSession session) {
        if (session == null) {
            return;
        }
        ChannelHandlerContext ctx = session.ctx();
        if (!ctx.channel().isActive()) {
            replicantSessions.remove(nodeId, session);
            return;
        }
        if (!session.pushing().compareAndSet(false, true)) {
            return;
        }
        try {
            long acked = session.ackedLogIndex().get();
            long sent = session.sentLogIndex().get();
            long inFlight = Math.max(0L, sent - acked);
            long remainingWindow = replicantMaxInFlightEvents - inFlight;
            if (remainingWindow <= 0) {
                return;
            }
            int budget = (int) Math.min((long) replicantPushBatchSize, remainingWindow);
            if (budget <= 0) {
                return;
            }
            long nextIndex = sent + 1L;
            int sentCount = 0;
            long lastSentIndex = sent;
            for (Map.Entry<Long, MetadataCommand> entry : eventHistory.tailMap(nextIndex, true).entrySet()) {
                if (sentCount >= budget) {
                    break;
                }
                ctx.write(new MetadataWireMessage(
                    MetadataMessageType.EVENT,
                    0L,
                    entry.getValue(),
                    entry.getKey(),
                    0L,
                    null,
                    true,
                    null,
                    null
                ));
                lastSentIndex = entry.getKey();
                sentCount++;
            }
            if (sentCount > 0) {
                final long flushUntil = lastSentIndex;
                session.sentLogIndex().updateAndGet(previous -> Math.max(previous, flushUntil));
                ctx.flush();
            }
        } finally {
            session.pushing().set(false);
        }
    }

    private void trimHistoryIfNeeded() {
        while (eventHistory.size() > maxReplayEvents) {
            Map.Entry<Long, MetadataCommand> entry = eventHistory.pollFirstEntry();
            if (entry == null) {
                break;
            }
        }
    }

    private final class CoreHandler extends SimpleChannelInboundHandler<MetadataWireMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MetadataWireMessage message) {
            if (message == null) {
                return;
            }
            if (message.type() == MetadataMessageType.SUBMIT_REQUEST) {
                onSubmit(ctx, message);
                return;
            }
            if (message.type() == MetadataMessageType.SUBSCRIBE_REQUEST) {
                onSubscribe(ctx, message);
                return;
            }
            if (message.type() == MetadataMessageType.ACK_REQUEST) {
                onAck(ctx, message);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            replicantSessions.entrySet().removeIf(entry -> {
                ReplicantSession session = entry.getValue();
                if (!Objects.equals(session.ctx(), ctx)) {
                    return false;
                }
                replicantAckLogIndex.computeIfAbsent(entry.getKey(), ignored -> new AtomicLong(0L))
                    .updateAndGet(previous -> Math.max(previous, session.ackedLogIndex().get()));
                scheduleNodeCleanup(entry.getKey());
                return true;
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    /**
     * Replicant 会话状态。
     * ackedLogIndex 表示对端已确认的日志位置，sentLogIndex 表示 CORE 已下发的位置。
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private record ReplicantSession(
        String nodeId,
        ChannelHandlerContext ctx,
        AtomicLong ackedLogIndex,
        AtomicLong sentLogIndex,
        AtomicBoolean pushing
    ) {
        private ReplicantSession(String nodeId, ChannelHandlerContext ctx, long ackedLogIndex, long sentLogIndex) {
            this(
                nodeId,
                ctx,
                new AtomicLong(ackedLogIndex),
                new AtomicLong(sentLogIndex),
                new AtomicBoolean(false)
            );
        }
    }
}
