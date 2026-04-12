package com.jmqx.store;

/**
 * Retained 存储参数。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class RetainedStoreProperties {
    // 是否启用保留消息 默认启用
    private boolean retainedEnabled = true;
    // 存储路径 默认 data/retained-rocksdb
    private String rocksdbPath = "data/retained-rocksdb";
    // 最大保留消息数量默认 100_000
    private int maxEntries = 100_000;
    // 最大保留消息字节数 默认 2M
    private long maxBytes = 2 * 1024 * 1024;
    // 最大保留消息负载字节数 默认 1M
    private int maxPayloadBytes = 1024 * 1024;
    // 溢出策略 默认 EVICT_LRU
    private RetainedOverflowStrategy overflowStrategy = RetainedOverflowStrategy.EVICT_LRU;

    public String getRocksdbPath() {
        return rocksdbPath;
    }

    public void setRocksdbPath(String rocksdbPath) {
        this.rocksdbPath = rocksdbPath;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public int getMaxPayloadBytes() {
        return maxPayloadBytes;
    }

    public void setMaxPayloadBytes(int maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public RetainedOverflowStrategy getOverflowStrategy() {
        return overflowStrategy;
    }

    public void setOverflowStrategy(RetainedOverflowStrategy overflowStrategy) {
        this.overflowStrategy = overflowStrategy;
    }

    public boolean isRetainedEnabled() {
        return retainedEnabled;
    }

    public void setRetainedEnabled(boolean retainedEnabled) {
        this.retainedEnabled = retainedEnabled;
    }
}
