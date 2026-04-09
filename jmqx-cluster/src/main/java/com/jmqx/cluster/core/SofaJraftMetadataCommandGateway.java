package com.jmqx.cluster.core;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;
import com.jmqx.cluster.MetadataLogApplier;
import com.jmqx.cluster.MetadataReplicator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SOFAJRaft 元数据写网关。
 * 使用 JRaft 自带日志存储保证元数据命令的一致复制与持久化。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class SofaJraftMetadataCommandGateway implements MetadataCommandGateway, MetadataReplicator {
    private static final Logger LOG = Logger.getLogger(SofaJraftMetadataCommandGateway.class.getName());

    private final String groupId;
    private final String serverId;
    private final String initialConf;
    private final String dataPath;
    private final int electionTimeoutMs;
    private final int snapshotIntervalSecs;
    private final int submitTimeoutMs;
    private final List<MetadataLogApplier> appliers = new CopyOnWriteArrayList<>();
    private final AtomicLong lastAppliedLogIndex = new AtomicLong(0L);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private RaftGroupService raftGroupService;
    private Node node;

    public SofaJraftMetadataCommandGateway(
        String groupId,
        String serverId,
        String initialConf,
        String dataPath,
        int electionTimeoutMs,
        int snapshotIntervalSecs,
        int submitTimeoutMs
    ) {
        this.groupId = (groupId == null || groupId.isBlank()) ? "jmqx-metadata" : groupId;
        this.serverId = serverId;
        this.initialConf = initialConf;
        this.dataPath = (dataPath == null || dataPath.isBlank()) ? "data/raft-metadata" : dataPath;
        this.electionTimeoutMs = Math.max(300, electionTimeoutMs);
        this.snapshotIntervalSecs = Math.max(10, snapshotIntervalSecs);
        this.submitTimeoutMs = Math.max(300, submitTimeoutMs);
    }

    public void registerApplier(MetadataLogApplier applier) {
        if (applier == null) {
            return;
        }
        appliers.add(applier);
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        try {
            ensureRaftDataDirs();
            PeerId peerId = JRaftUtils.getPeerId(serverId);
            Configuration conf = JRaftUtils.getConfiguration(initialConf);
            NodeOptions options = new NodeOptions();
            options.setFsm(new MetadataFsm(appliers, lastAppliedLogIndex));
            options.setInitialConf(conf);
            options.setElectionTimeoutMs(electionTimeoutMs);
            options.setSnapshotIntervalSecs(snapshotIntervalSecs);
            options.setLogUri(Path.of(dataPath, "log").toString());
            options.setRaftMetaUri(Path.of(dataPath, "meta").toString());
            options.setSnapshotUri(Path.of(dataPath, "snapshot").toString());
            raftGroupService = new RaftGroupService(groupId, peerId, options);
            node = raftGroupService.start();
            LOG.info(() -> "[CLUSTER][CORE][RAFT] started groupId=" + groupId + ", serverId=" + serverId
                + ", initialConf=" + initialConf + ", dataPath=" + dataPath);
        } catch (Exception exception) {
            started.set(false);
            throw new IllegalStateException("failed to start sofa-jraft metadata gateway", exception);
        }
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (node != null) {
            node.shutdown();
            node = null;
        }
        if (raftGroupService != null) {
            raftGroupService.shutdown();
            raftGroupService = null;
        }
        LOG.info("[CLUSTER][CORE][RAFT] stopped");
    }

    @Override
    public long submit(MetadataCommand command) {
        if (command == null || !started.get() || node == null) {
            return -1L;
        }
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            Task task = new Task();
            task.setData(ByteBuffer.wrap(CommandCodec.encode(command)));
            task.setDone((Closure) status -> handleClosure(status, future));
            node.apply(task);
            return future.get(submitTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return -1L;
        } catch (ExecutionException | TimeoutException exception) {
            LOG.warning("[CLUSTER][CORE][RAFT] submit failed, error=" + exception.getMessage());
            return -1L;
        } catch (Exception exception) {
            LOG.warning("[CLUSTER][CORE][RAFT] submit failed, error=" + exception.getMessage());
            return -1L;
        }
    }

    /**
     * 返回当前已知 leader 地址，格式为 host:port。
     * follower 节点可通过该地址引导客户端重定向。
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    public String leaderEndpoint() {
        if (node == null) {
            return null;
        }
        PeerId leader = node.getLeaderId();
        if (leader == null || leader.getEndpoint() == null) {
            return null;
        }
        return leader.getEndpoint().toString();
    }

    private void handleClosure(Status status, CompletableFuture<Long> future) {
        if (status == null || status.isOk()) {
            future.complete(lastAppliedLogIndex.get());
            return;
        }
        future.complete(-1L);
        if (status.getRaftError() == RaftError.EPERM) {
            LOG.warning("[CLUSTER][CORE][RAFT] submit rejected because node is not leader");
            return;
        }
        LOG.warning("[CLUSTER][CORE][RAFT] submit closure error, code="
            + status.getCode() + ", message=" + status.getErrorMsg());
    }

    private void ensureRaftDataDirs() throws Exception {
        Files.createDirectories(Path.of(dataPath, "log"));
        Files.createDirectories(Path.of(dataPath, "meta"));
        Files.createDirectories(Path.of(dataPath, "snapshot"));
    }

    /**
     * 元数据状态机。
     * 负责按 raft 提交顺序应用命令并回调读模型更新器。
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private static final class MetadataFsm extends StateMachineAdapter {
        private final List<MetadataLogApplier> appliers;
        private final AtomicLong lastAppliedLogIndex;

        private MetadataFsm(List<MetadataLogApplier> appliers, AtomicLong lastAppliedLogIndex) {
            this.appliers = appliers;
            this.lastAppliedLogIndex = lastAppliedLogIndex;
        }

        @Override
        public void onApply(com.alipay.sofa.jraft.Iterator iterator) {
            while (iterator.hasNext()) {
                MetadataCommand command = CommandCodec.decode(iterator.getData());
                long logIndex = iterator.getIndex();
                if (command != null) {
                    for (MetadataLogApplier applier : appliers) {
                        try {
                            applier.apply(logIndex, command);
                        } catch (Exception exception) {
                            LOG.log(Level.WARNING, "[CLUSTER][CORE][RAFT] apply callback failed", exception);
                        }
                    }
                    lastAppliedLogIndex.updateAndGet(current -> Math.max(current, logIndex));
                }
                Closure done = iterator.done();
                if (done != null) {
                    done.run(Status.OK());
                }
                iterator.next();
            }
        }

        @Override
        public void onSnapshotSave(SnapshotWriter writer, Closure done) {
            done.run(Status.OK());
        }

        @Override
        public boolean onSnapshotLoad(SnapshotReader reader) {
            return true;
        }
    }

    /**
     * 元数据命令编解码器。
     * 采用轻量二进制格式，避免额外 JSON 依赖。
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private static final class CommandCodec {
        private static byte[] encode(MetadataCommand command) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream(256);
                DataOutputStream dataOutput = new DataOutputStream(out);
                writeNullable(dataOutput, command.namespace());
                writeNullable(dataOutput, command.operation());
                writeNullable(dataOutput, command.key());
                writeNullable(dataOutput, command.value());
                writeNullable(dataOutput, command.sourceNodeId());
                dataOutput.flush();
                return out.toByteArray();
            } catch (Exception exception) {
                throw new IllegalStateException("encode metadata command failed", exception);
            }
        }

        private static MetadataCommand decode(ByteBuffer buffer) {
            if (buffer == null) {
                return null;
            }
            try {
                ByteBuffer copy = buffer.slice();
                byte[] bytes = new byte[copy.remaining()];
                copy.get(bytes);
                DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
                return new MetadataCommand(
                    readNullable(input),
                    readNullable(input),
                    readNullable(input),
                    readNullable(input),
                    readNullable(input)
                );
            } catch (Exception exception) {
                LOG.warning("[CLUSTER][CORE][RAFT] decode metadata command failed: " + exception.getMessage());
                return null;
            }
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
    }
}
