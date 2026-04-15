package com.jmqx.store.rocksdb;

import com.jmqx.store.qos.Qos2InboundInflightMessage;
import com.jmqx.store.qos.Qos2InflightStore;
import com.jmqx.store.qos.Qos2OutboundInflightMessage;

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
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 RocksDB 的 QoS2 inflight 持久化存储。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public class RocksDbQos2InflightStore implements Qos2InflightStore {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger LOG = Logger.getLogger(RocksDbQos2InflightStore.class.getName());
    private static final byte KEY_KIND_OUTBOUND = 'o';
    private static final byte KEY_KIND_INBOUND = 'i';
    private static final byte VALUE_OUTBOUND_V1 = 1;
    private static final byte VALUE_OUTBOUND_V2 = 3;
    private static final byte VALUE_INBOUND = 2;

    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksDbQos2InflightStore(String dbPath) {
        String path = normalizePath(dbPath);
        ensureParentDirectory(path);
        try {
            this.options = new Options().setCreateIfMissing(true);
            this.writeOptions = new WriteOptions();
            this.db = RocksDB.open(options, path);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("open qos2 inflight rocksdb failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public void saveOutbound(String clientId, Qos2OutboundInflightMessage message) {
        if (clientId == null || clientId.isBlank() || message == null) {
            return;
        }
        try {
            db.put(buildKey(KEY_KIND_OUTBOUND, clientId, message.packetId()), encodeOutbound(message));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[QOS2][STORE] save outbound failed clientId=" + clientId
                + ", packetId=" + message.packetId() + ", error=" + exception.getMessage(), exception);
        }
    }

    @Override
    public void removeOutbound(String clientId, int packetId) {
        deleteKey(buildKey(KEY_KIND_OUTBOUND, clientId, packetId), "[QOS2][STORE] remove outbound failed");
    }

    @Override
    public void removeOutboundClient(String clientId) {
        deleteByPrefix(clientPrefix(KEY_KIND_OUTBOUND, clientId), "[QOS2][STORE] remove outbound client failed");
    }

    @Override
    public List<Qos2OutboundInflightMessage> listOutbound(String clientId) {
        List<Qos2OutboundInflightMessage> result = new ArrayList<>();
        if (clientId == null || clientId.isBlank()) {
            return result;
        }
        byte[] prefix = clientPrefix(KEY_KIND_OUTBOUND, clientId);
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                Qos2OutboundInflightMessage message = decodeOutbound(iterator.value());
                if (message != null) {
                    result.add(message);
                }
            }
        }
        return result;
    }

    @Override
    public int maxOutboundPacketId(String clientId) {
        int max = 0;
        for (Qos2OutboundInflightMessage message : listOutbound(clientId)) {
            if (message.packetId() > max) {
                max = message.packetId();
            }
        }
        return max;
    }

    @Override
    public void saveInbound(String clientId, Qos2InboundInflightMessage message) {
        if (clientId == null || clientId.isBlank() || message == null) {
            return;
        }
        try {
            db.put(buildKey(KEY_KIND_INBOUND, clientId, message.packetId()), encodeInbound(message));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[QOS2][STORE] save inbound failed clientId=" + clientId
                + ", packetId=" + message.packetId() + ", error=" + exception.getMessage(), exception);
        }
    }

    @Override
    public void removeInbound(String clientId, int packetId) {
        deleteKey(buildKey(KEY_KIND_INBOUND, clientId, packetId), "[QOS2][STORE] remove inbound failed");
    }

    @Override
    public void removeInboundClient(String clientId) {
        deleteByPrefix(clientPrefix(KEY_KIND_INBOUND, clientId), "[QOS2][STORE] remove inbound client failed");
    }

    @Override
    public Optional<Qos2InboundInflightMessage> getInbound(String clientId, int packetId) {
        if (clientId == null || clientId.isBlank() || packetId <= 0) {
            return Optional.empty();
        }
        try {
            byte[] value = db.get(buildKey(KEY_KIND_INBOUND, clientId, packetId));
            return Optional.ofNullable(decodeInbound(value));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[QOS2][STORE] get inbound failed clientId=" + clientId
                + ", packetId=" + packetId + ", error=" + exception.getMessage(), exception);
            return Optional.empty();
        }
    }

    @Override
    public List<Qos2InboundInflightMessage> listInbound(String clientId) {
        List<Qos2InboundInflightMessage> result = new ArrayList<>();
        if (clientId == null || clientId.isBlank()) {
            return result;
        }
        byte[] prefix = clientPrefix(KEY_KIND_INBOUND, clientId);
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                Qos2InboundInflightMessage message = decodeInbound(iterator.value());
                if (message != null) {
                    result.add(message);
                }
            }
        }
        return result;
    }

    @Override
    public void close() {
        db.close();
        writeOptions.close();
        options.close();
    }

    private void deleteKey(byte[] key, String logPrefix) {
        if (key == null) {
            return;
        }
        try {
            db.delete(key);
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, logPrefix + ", error=" + exception.getMessage(), exception);
        }
    }

    private void deleteByPrefix(byte[] prefix, String logPrefix) {
        if (prefix == null || prefix.length == 0) {
            return;
        }
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
            LOG.log(Level.WARNING, logPrefix + ", error=" + exception.getMessage(), exception);
        }
    }

    private static byte[] buildKey(byte kind, String clientId, int packetId) {
        if (clientId == null || clientId.isBlank() || packetId <= 0) {
            return null;
        }
        byte[] prefix = clientPrefix(kind, clientId);
        ByteBuffer buffer = ByteBuffer.allocate(prefix.length + 4);
        buffer.put(prefix);
        buffer.putInt(packetId);
        return buffer.array();
    }

    private static byte[] clientPrefix(byte kind, String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return new byte[0];
        }
        byte[] id = clientId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + id.length + 1);
        buffer.put(kind);
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

    private static byte[] encodeOutbound(Qos2OutboundInflightMessage message) {
        byte[] topicBytes = message.topic() == null ? new byte[0] : message.topic().getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = message.payload() == null ? new byte[0] : message.payload();
        // v2: state 压缩为 1 byte，降低 inflight value 占用。
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 8 + 1 + 4 + topicBytes.length + 4 + payloadBytes.length);
        buffer.put(VALUE_OUTBOUND_V2);
        buffer.putInt(message.packetId());
        buffer.putLong(message.lastSentAtMs());
        buffer.put((byte) (message.state() & 0xFF));
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(payloadBytes.length);
        buffer.put(payloadBytes);
        return buffer.array();
    }

    private static Qos2OutboundInflightMessage decodeOutbound(byte[] raw) {
        if (raw == null || raw.length < 1) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(raw);
            byte marker = buffer.get();
            if (marker != VALUE_OUTBOUND_V1 && marker != VALUE_OUTBOUND_V2) {
                return null;
            }
            int packetId = buffer.getInt();
            long lastSentAtMs = buffer.getLong();
            int state = marker == VALUE_OUTBOUND_V2
                ? Byte.toUnsignedInt(buffer.get())
                : buffer.getInt();
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
            return new Qos2OutboundInflightMessage(
                packetId,
                new String(topicBytes, StandardCharsets.UTF_8),
                payloadBytes,
                state,
                lastSentAtMs
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeInbound(Qos2InboundInflightMessage message) {
        byte[] topicBytes = message.topic() == null ? new byte[0] : message.topic().getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = message.payload() == null ? new byte[0] : message.payload();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 1 + 1 + 4 + topicBytes.length + 4 + payloadBytes.length);
        buffer.put(VALUE_INBOUND);
        buffer.putInt(message.packetId());
        buffer.put((byte) (message.retain() ? 1 : 0));
        buffer.put(message.state());
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(payloadBytes.length);
        buffer.put(payloadBytes);
        return buffer.array();
    }

    private static Qos2InboundInflightMessage decodeInbound(byte[] raw) {
        if (raw == null || raw.length < 1) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(raw);
            if (buffer.get() != VALUE_INBOUND) {
                return null;
            }
            int packetId = buffer.getInt();
            boolean retain = buffer.get() != 0;
            byte state = 0;
            int topicLen;
            if (buffer.remaining() >= 5) {
                buffer.mark();
                byte maybeState = buffer.get();
                int maybeTopicLen = buffer.getInt();
                if ((maybeState == 0 || maybeState == 1) && maybeTopicLen >= 0 && maybeTopicLen <= buffer.remaining()) {
                    state = maybeState;
                    topicLen = maybeTopicLen;
                } else {
                    buffer.reset();
                    topicLen = buffer.getInt();
                }
            } else {
                topicLen = buffer.getInt();
            }
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
            return new Qos2InboundInflightMessage(
                packetId,
                new String(topicBytes, StandardCharsets.UTF_8),
                payloadBytes,
                retain,
                state
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String normalizePath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return "data/qos2-inflight-rocksdb";
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
            throw new IllegalStateException("create qos2 inflight rocksdb parent dir failed: " + parent, exception);
        }
    }
}
