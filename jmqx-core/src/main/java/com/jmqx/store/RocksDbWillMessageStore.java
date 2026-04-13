package com.jmqx.store;

import com.jmqx.broker.core.WillMessage;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 RocksDB 的遗嘱消息持久化存储。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public class RocksDbWillMessageStore implements WillMessageStore {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger LOG = Logger.getLogger(RocksDbWillMessageStore.class.getName());

    private final Options options;
    private final RocksDB db;

    public RocksDbWillMessageStore(String dbPath) {
        String path = normalizePath(dbPath);
        ensureParentDirectory(path);
        try {
            this.options = new Options().setCreateIfMissing(true);
            this.db = RocksDB.open(options, path);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("open will-message rocksdb failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public void save(String clientId, WillMessage willMessage) {
        if (isBlank(clientId) || willMessage == null || isBlank(willMessage.topic())) {
            return;
        }
        try {
            db.put(key(clientId), encode(willMessage));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[WILL][STORE] save failed clientId=" + clientId
                + ", error=" + exception.getMessage(), exception);
        }
    }

    @Override
    public Optional<WillMessage> get(String clientId) {
        if (isBlank(clientId)) {
            return Optional.empty();
        }
        try {
            byte[] raw = db.get(key(clientId));
            return Optional.ofNullable(decode(raw));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[WILL][STORE] get failed clientId=" + clientId
                + ", error=" + exception.getMessage(), exception);
            return Optional.empty();
        }
    }

    @Override
    public void remove(String clientId) {
        if (isBlank(clientId)) {
            return;
        }
        try {
            db.delete(key(clientId));
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "[WILL][STORE] remove failed clientId=" + clientId
                + ", error=" + exception.getMessage(), exception);
        }
    }

    @Override
    public void close() {
        db.close();
        options.close();
    }

    private static byte[] key(String clientId) {
        return clientId.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] encode(WillMessage willMessage) {
        byte[] topicBytes = willMessage.topic().getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = willMessage.payload() == null ? new byte[0] : willMessage.payload();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 1 + 1 + 4 + topicBytes.length + 4 + payloadBytes.length);
        buffer.put((byte) 1);
        buffer.put((byte) willMessage.qos());
        buffer.put((byte) (willMessage.retain() ? 1 : 0));
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(payloadBytes.length);
        buffer.put(payloadBytes);
        return buffer.array();
    }

    private static WillMessage decode(byte[] raw) {
        if (raw == null || raw.length < 1) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(raw);
            byte version = buffer.get();
            if (version != 1) {
                return null;
            }
            int qos = Byte.toUnsignedInt(buffer.get());
            boolean retain = buffer.get() == 1;
            int topicLength = buffer.getInt();
            if (topicLength < 0 || topicLength > buffer.remaining()) {
                return null;
            }
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            int payloadLength = buffer.getInt();
            if (payloadLength < 0 || payloadLength > buffer.remaining()) {
                return null;
            }
            byte[] payloadBytes = new byte[payloadLength];
            buffer.get(payloadBytes);
            return new WillMessage(
                new String(topicBytes, StandardCharsets.UTF_8),
                payloadBytes,
                qos,
                retain
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String normalizePath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return "data/will-rocksdb";
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
            throw new IllegalStateException("create will rocksdb parent dir failed: " + parent, exception);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}

