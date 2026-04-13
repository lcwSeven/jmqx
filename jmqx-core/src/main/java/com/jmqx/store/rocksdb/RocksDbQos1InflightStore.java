package com.jmqx.store.rocksdb;

import com.jmqx.store.qos.Qos1InflightMessage;
import com.jmqx.store.qos.Qos1InflightStore;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 RocksDB 的 QoS1 inflight 持久化存储。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class RocksDbQos1InflightStore implements Qos1InflightStore {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger LOG = Logger.getLogger(RocksDbQos1InflightStore.class.getName());

    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksDbQos1InflightStore(String dbPath) {
        String path = normalizePath(dbPath);
        ensureParentDirectory(path);
        try {
            this.options = new Options().setCreateIfMissing(true);
            this.writeOptions = new WriteOptions();
            this.db = RocksDB.open(options, path);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("open qos1 inflight rocksdb failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public void save(String clientId, Qos1InflightMessage message) {
        if (clientId == null || clientId.isBlank() || message == null) {
            return;
        }
        try {
            db.put(buildKey(clientId, message.packetId()), encode(message));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[QOS1][STORE] save failed clientId=" + clientId
                + ", packetId=" + message.packetId() + ", error=" + exception.getMessage(), exception);
        }
    }

    @Override
    public void remove(String clientId, int packetId) {
        if (clientId == null || clientId.isBlank() || packetId <= 0) {
            return;
        }
        try {
            db.delete(buildKey(clientId, packetId));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[QOS1][STORE] remove failed clientId=" + clientId
                + ", packetId=" + packetId + ", error=" + exception.getMessage(), exception);
        }
    }

    @Override
    public void removeClient(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        byte[] prefix = clientPrefix(clientId);
        List<byte[]> keys = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                keys.add(key.clone());
            }
        }
        if (keys.isEmpty()) {
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            for (byte[] key : keys) {
                batch.delete(key);
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[QOS1][STORE] remove client batch failed clientId=" + clientId, exception);
        }
    }

    @Override
    public List<Qos1InflightMessage> listByClient(String clientId) {
        List<Qos1InflightMessage> result = new ArrayList<>();
        if (clientId == null || clientId.isBlank()) {
            return result;
        }
        byte[] prefix = clientPrefix(clientId);
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                Qos1InflightMessage message = decode(iterator.value());
                if (message != null) {
                    result.add(message);
                }
            }
        }
        return result;
    }

    @Override
    public int maxPacketId(String clientId) {
        int max = 0;
        for (Qos1InflightMessage message : listByClient(clientId)) {
            if (message.packetId() > max) {
                max = message.packetId();
            }
        }
        return max;
    }

    @Override
    public void close() {
        db.close();
        writeOptions.close();
        options.close();
    }

    private static byte[] buildKey(String clientId, int packetId) {
        byte[] prefix = clientPrefix(clientId);
        ByteBuffer buffer = ByteBuffer.allocate(prefix.length + 4);
        buffer.put(prefix);
        buffer.putInt(packetId);
        return buffer.array();
    }

    private static byte[] clientPrefix(String clientId) {
        byte[] id = clientId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(id.length + 1);
        buffer.put(id);
        buffer.put((byte) 0);
        return buffer.array();
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value == null || prefix == null || value.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] encode(Qos1InflightMessage message) {
        byte[] topicBytes = message.topic() == null ? new byte[0] : message.topic().getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = message.payload() == null ? new byte[0] : message.payload();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4 + topicBytes.length + 4 + payloadBytes.length);
        buffer.put((byte) 1);
        buffer.putInt(message.packetId());
        buffer.putLong(message.lastSentAtMs());
        buffer.putInt(message.retryCount());
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(payloadBytes.length);
        buffer.put(payloadBytes);
        return buffer.array();
    }

    private static Qos1InflightMessage decode(byte[] raw) {
        if (raw == null || raw.length < 1) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(raw);
            byte version = buffer.get();
            if (version != 1) {
                return null;
            }
            int packetId = buffer.getInt();
            long lastSentAtMs = buffer.getLong();
            int retryCount = buffer.getInt();
            int topicLen = buffer.getInt();
            if (topicLen < 0 || topicLen > buffer.remaining()) {
                return null;
            }
            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            int payloadLen = buffer.getInt();
            if (payloadLen < 0 || payloadLen > buffer.remaining()) {
                return null;
            }
            byte[] payloadBytes = new byte[payloadLen];
            buffer.get(payloadBytes);
            return new Qos1InflightMessage(
                packetId,
                new String(topicBytes, StandardCharsets.UTF_8),
                payloadBytes,
                lastSentAtMs,
                retryCount
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String normalizePath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return "data/qos1-inflight-rocksdb";
        }
        return rawPath.trim();
    }

    private static void ensureParentDirectory(String dbPath) {
        Path path = Path.of(dbPath).toAbsolutePath();
        Path parent = path.getParent();
        if (parent == null) {
            return;
        }
        try {
            Files.createDirectories(parent);
        } catch (Exception exception) {
            throw new IllegalStateException("create qos1 inflight rocksdb parent dir failed: " + parent, exception);
        }
    }
}
