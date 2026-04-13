package com.jmqx.common.logging;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端日志追踪任务管理器。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public final class ClientTraceManager {

    public static final long MAX_DURATION_MILLIS = 30L * 60L * 1000L;
    private static final ClientTraceManager INSTANCE = new ClientTraceManager();
    private static final DateTimeFormatter FILE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
            .withZone(ZoneId.systemDefault());

    private final Map<String, ClientTraceTask> tasks = new ConcurrentHashMap<>();

    private ClientTraceManager() {
    }

    public static ClientTraceManager getInstance() {
        return INSTANCE;
    }

    public void upsert(ClientTraceTask task) {
        if (task == null || task.id() == null || task.id().isBlank()) {
            return;
        }
        tasks.put(task.id(), task.normalize());
    }

    public void remove(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        tasks.remove(taskId);
    }

    public ClientTraceTask get(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return null;
        }
        return tasks.get(taskId);
    }

    public List<ClientTraceTask> list() {
        List<ClientTraceTask> values = new ArrayList<>(tasks.values());
        values.sort(Comparator.comparing(ClientTraceTask::startAt).reversed());
        return values;
    }

    public List<ClientTraceTask> findActiveTasks(String clientId, long timestamp) {
        if (clientId == null || clientId.isBlank()) {
            return List.of();
        }
        List<ClientTraceTask> matched = new ArrayList<>();
        for (ClientTraceTask task : tasks.values()) {
            if (!clientId.equals(task.clientId())) {
                continue;
            }
            if (timestamp < task.startAt() || timestamp > task.endAt()) {
                continue;
            }
            matched.add(task);
        }
        matched.sort(Comparator.comparing(ClientTraceTask::startAt));
        return matched;
    }

    public String generateFilePath(String clientId, long startAt, String taskId) {
        String safeClientId = sanitize(clientId);
        String safeTaskId = sanitize(taskId);
        String fileName = safeClientId + "-" + FILE_TIME_FORMATTER.format(Instant.ofEpochMilli(startAt)) + "-" + safeTaskId + ".log";
        Path path = Paths.get("logs", "client-traces", fileName).toAbsolutePath();
        return path.toString();
    }

    public static String newTaskId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static String sanitize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.trim().replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    public record ClientTraceTask(
            String id,
            String clientId,
            long startAt,
            long endAt,
            long createdAt,
            String createdBy,
            String filePath
    ) {
        public ClientTraceTask normalize() {
            return new ClientTraceTask(
                    id == null ? "" : id.trim(),
                    clientId == null ? "" : clientId.trim(),
                    Math.max(0L, startAt),
                    Math.max(0L, endAt),
                    Math.max(0L, createdAt),
                    createdBy == null ? "" : createdBy.trim(),
                    filePath == null ? "" : filePath.trim()
            );
        }

        public String statusAt(long now) {
            if (now < startAt) {
                return "scheduled";
            }
            if (now <= endAt) {
                return "active";
            }
            return "expired";
        }

        public long durationMillis() {
            return Math.max(0L, endAt - startAt);
        }
    }
}
