package com.jmqx.store.rocksdb;

import com.jmqx.common.TopicMatcher;
import com.jmqx.store.retained.RetainedMessage;
import com.jmqx.store.retained.RetainedMessageStore;
import com.jmqx.store.retained.RetainedOverflowStrategy;
import com.jmqx.store.retained.RetainedStoreMetrics;
import com.jmqx.store.retained.RetainedStoreProperties;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 RocksDB 的 Retained 存储实现。
 * 数据全量落盘，内存仅维护受控热缓存，避免 retained 数据增长导致堆内存爆炸。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class RocksDbRetainedMessageStore implements RetainedMessageStore {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger LOG = Logger.getLogger(RocksDbRetainedMessageStore.class.getName());
    private static final int ESTIMATED_RECORD_OVERHEAD = 96;

    private final RetainedStoreProperties properties;
    private final Options options;
    private final RocksDB db;

    /**
     * RocksDB 之上的热缓存，access-order 用于 LRU。
     */
    private final LinkedHashMap<String, RetainedMessage> cache = new LinkedHashMap<>(16, 0.75f, true);
    private final AtomicLong evictedCount = new AtomicLong(0);
    private final AtomicLong rejectedTooLargeCount = new AtomicLong(0);
    private final AtomicLong rejectedCapacityCount = new AtomicLong(0);
    private final AtomicLong updateCount = new AtomicLong(0);
    private long cacheBytes = 0;
    private final AtomicLong persistedEntries = new AtomicLong(0);
    private final AtomicLong persistedBytes = new AtomicLong(0);

    public RocksDbRetainedMessageStore(RetainedStoreProperties properties) {
        this.properties = properties == null ? new RetainedStoreProperties() : properties;
        String dbPath = normalizeDbPath(this.properties.getRocksdbPath());
        ensureParentDirectory(dbPath);
        try {
            this.options = new Options().setCreateIfMissing(true);
            this.db = RocksDB.open(options, dbPath);
            initializeStatistics();
            LOG.info(() -> "[RETAINED][ROCKSDB] opened path=" + dbPath + ", entries=" + persistedEntries.get());
        } catch (RocksDBException e) {
            throw new IllegalStateException("open rocksdb retained store failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void saveOrRemove(RetainedMessage message) {
        // 判断是否支持保留消息 如果不支持支持保留消息 则直接返回
        if (!properties.isRetainedEnabled()) {
            return;
        }
        // 判断 topic 是否为空
        if (message == null || message.topic() == null || message.topic().isBlank()) {
            return;
        }

        String topic = message.topic();
        byte[] topicKey = topic.getBytes(StandardCharsets.UTF_8);
        updateCount.incrementAndGet();

        try {
            byte[] old = db.get(topicKey);
            if (message.payload().length == 0) {
                if (old != null) {
                    db.delete(topicKey);
                    persistedEntries.decrementAndGet();
                    persistedBytes.addAndGet(-estimatePersistedSize(topic, old));
                }
                removeFromCache(topic);
                return;
            }

            int maxPayloadBytes = Math.max(properties.getMaxPayloadBytes(), 1);
            if (message.payload().length > maxPayloadBytes) {
                rejectedTooLargeCount.incrementAndGet();
                LOG.warning("[RETAINED][ROCKSDB] reject too large topic=" + topic
                    + ", payloadBytes=" + message.payload().length
                    + ", maxPayloadBytes=" + maxPayloadBytes);
                return;
            }

            RetainedMessage stored = new RetainedMessage(
                topic,
                message.payload().clone(),
                message.qos(),
                message.retain()
            );
            byte[] encoded = encode(stored);
            db.put(topicKey, encoded);

            if (old == null) {
                persistedEntries.incrementAndGet();
            } else {
                persistedBytes.addAndGet(-estimatePersistedSize(topic, old));
            }
            persistedBytes.addAndGet(estimatePersistedSize(topic, encoded));
            putToCache(stored);
        } catch (RocksDBException e) {
            LOG.log(Level.WARNING, "[RETAINED][ROCKSDB] write failed topic=" + topic + ", error=" + e.getMessage(), e);
        }
    }

    @Override
    public List<RetainedMessage> findByTopicFilter(String topicFilter) {
        List<RetainedMessage> result = new ArrayList<>();
        if (topicFilter == null || topicFilter.isBlank()) {
            return result;
        }

        if (!containsWildcard(topicFilter)) {
            RetainedMessage cached = getFromCache(topicFilter);
            if (cached != null) {
                result.add(cached);
                return result;
            }
            try {
                byte[] raw = db.get(topicFilter.getBytes(StandardCharsets.UTF_8));
                if (raw != null) {
                    RetainedMessage message = decode(topicFilter, raw);
                    result.add(message);
                    putToCache(message);
                }
            } catch (RocksDBException e) {
                LOG.log(Level.WARNING, "[RETAINED][ROCKSDB] read failed topicFilter=" + topicFilter + ", error=" + e.getMessage(), e);
            }
            return result;
        }

        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String topic = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!TopicMatcher.matches(topicFilter, topic)) {
                    continue;
                }
                try {
                    RetainedMessage message = decode(topic, iterator.value());
                    result.add(message);
                    putToCache(message);
                } catch (Exception e) {
                    LOG.warning("[RETAINED][ROCKSDB] decode failed topic=" + topic + ", error=" + e.getMessage());
                }
            }
        }
        return result;
    }

    @Override
    public RetainedStoreMetrics metrics() {
        return new RetainedStoreMetrics(
            (int) persistedEntries.get(),
            persistedBytes.get(),
            evictedCount.get(),
            rejectedTooLargeCount.get(),
            rejectedCapacityCount.get(),
            updateCount.get()
        );
    }

    @Override
    public void close() {
        synchronized (cache) {
            cache.clear();
            cacheBytes = 0;
        }
        db.close();
        options.close();
    }

    private void initializeStatistics() {
        long count = 0;
        long bytes = 0;
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
                String topic = new String(iterator.key(), StandardCharsets.UTF_8);
                bytes += estimatePersistedSize(topic, iterator.value());
            }
        }
        persistedEntries.set(count);
        persistedBytes.set(bytes);
    }

    private void putToCache(RetainedMessage message) {
        synchronized (cache) {
            RetainedMessage existing = cache.remove(message.topic());
            if (existing != null) {
                cacheBytes -= estimateCacheSize(existing);
            }
            long incoming = estimateCacheSize(message);
            if (!ensureCacheCapacity(incoming)) {
                rejectedCapacityCount.incrementAndGet();
                return;
            }
            cache.put(message.topic(), message);
            cacheBytes += incoming;
        }
    }

    private void removeFromCache(String topic) {
        synchronized (cache) {
            RetainedMessage existing = cache.remove(topic);
            if (existing != null) {
                cacheBytes -= estimateCacheSize(existing);
            }
        }
    }

    private RetainedMessage getFromCache(String topic) {
        synchronized (cache) {
            return cache.get(topic);
        }
    }

    private boolean ensureCacheCapacity(long incomingSize) {
        int maxEntries = Math.max(properties.getMaxEntries(), 1);
        long maxBytes = Math.max(properties.getMaxBytes(), 1L);
        if (properties.getOverflowStrategy() == RetainedOverflowStrategy.REJECT_NEW) {
            return cache.size() + 1 <= maxEntries && cacheBytes + incomingSize <= maxBytes;
        }
        while (cache.size() + 1 > maxEntries || cacheBytes + incomingSize > maxBytes) {
            if (!evictOneCacheEntry()) {
                return false;
            }
        }
        return true;
    }

    private boolean evictOneCacheEntry() {
        Iterator<Map.Entry<String, RetainedMessage>> iterator = cache.entrySet().iterator();
        if (!iterator.hasNext()) {
            return false;
        }
        Map.Entry<String, RetainedMessage> eldest = iterator.next();
        iterator.remove();
        cacheBytes -= estimateCacheSize(eldest.getValue());
        evictedCount.incrementAndGet();
        return true;
    }

    private static byte[] encode(RetainedMessage message) {
        byte[] payload = message.payload();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 1 + 4 + payload.length);
        buffer.put((byte) 1); // version
        buffer.putInt(message.qos());
        buffer.put((byte) (message.retain() ? 1 : 0));
        buffer.putInt(payload.length);
        buffer.put(payload);
        return buffer.array();
    }

    private static RetainedMessage decode(String topic, byte[] raw) {
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        byte version = buffer.get();
        if (version != 1) {
            throw new IllegalStateException("unsupported retained record version: " + version);
        }
        int qos = buffer.getInt();
        boolean retain = buffer.get() == 1;
        int payloadLength = buffer.getInt();
        byte[] payload = new byte[payloadLength];
        buffer.get(payload);
        return new RetainedMessage(topic, payload, qos, retain);
    }

    private static boolean containsWildcard(String topicFilter) {
        return topicFilter.indexOf('+') >= 0 || topicFilter.indexOf('#') >= 0;
    }

    private static long estimateCacheSize(RetainedMessage message) {
        long topicBytes = message.topic().getBytes(StandardCharsets.UTF_8).length;
        return topicBytes + message.payload().length + ESTIMATED_RECORD_OVERHEAD;
    }

    private static long estimatePersistedSize(String topic, byte[] encodedValue) {
        long topicBytes = topic.getBytes(StandardCharsets.UTF_8).length;
        return topicBytes + encodedValue.length + ESTIMATED_RECORD_OVERHEAD;
    }

    private static String normalizeDbPath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return "data/retained-rocksdb";
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
        } catch (Exception e) {
            throw new IllegalStateException("create rocksdb parent dir failed: " + parent, e);
        }
    }
}
