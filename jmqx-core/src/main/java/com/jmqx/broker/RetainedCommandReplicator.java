package com.jmqx.broker;

import com.jmqx.cluster.MetadataCommand;
import com.jmqx.cluster.MetadataCommandGateway;

import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * Retained 元数据提交与失败重试协调器。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class RetainedCommandReplicator {
    private static final String RETAINED_NAMESPACE = "retained.message";
    private static final String OP_RETAINED_UPSERT = "upsert";
    private static final String OP_RETAINED_REMOVE = "remove";

    private final MetadataCommandGateway metadataCommandGateway;
    private final String nodeId;
    private final Logger logger;
    private final ConcurrentMap<String, MetadataCommand> pendingRetainedCommands = new ConcurrentHashMap<>();

    public RetainedCommandReplicator(MetadataCommandGateway metadataCommandGateway, String nodeId, Logger logger) {
        this.metadataCommandGateway = metadataCommandGateway;
        this.nodeId = (nodeId == null || nodeId.isBlank()) ? "node-1" : nodeId;
        this.logger = logger == null ? Logger.getLogger(RetainedCommandReplicator.class.getName()) : logger;
    }

    public void submitRetainedWithClusterSync(String topic, byte[] payload, int qos) {
        MetadataCommand command = buildRetainedCommand(topic, payload, qos);
        if (command == null) {
            return;
        }
        if (submitRetainedCommand(command)) {
            pendingRetainedCommands.remove(topic);
            return;
        }
        pendingRetainedCommands.put(topic, command);
    }

    public void retryPendingCommands() {
        if (pendingRetainedCommands.isEmpty()) {
            return;
        }
        pendingRetainedCommands.forEach((topic, command) -> {
            long committed = metadataCommandGateway.submit(command);
            if (committed >= 0) {
                pendingRetainedCommands.remove(topic, command);
            }
        });
    }

    private MetadataCommand buildRetainedCommand(String topic, byte[] payload, int qos) {
        if (topic == null || topic.isBlank()) {
            return null;
        }
        byte[] safePayload = payload == null ? new byte[0] : payload;
        String operation = safePayload.length == 0 ? OP_RETAINED_REMOVE : OP_RETAINED_UPSERT;
        String value = safePayload.length == 0
                ? null
                : qos + "|" + Base64.getEncoder().encodeToString(safePayload);
        return new MetadataCommand(
                RETAINED_NAMESPACE,
                operation,
                topic,
                value,
                nodeId
        );
    }

    private boolean submitRetainedCommand(MetadataCommand command) {
        if (command == null) {
            return false;
        }
        long committedIndex = metadataCommandGateway.submit(command);
        if (committedIndex >= 0) {
            return true;
        }
        logger.warning(() -> "[CLUSTER] retained metadata submit failed, topic=" + command.key()
                + ", operation=" + command.operation());
        return false;
    }
}
