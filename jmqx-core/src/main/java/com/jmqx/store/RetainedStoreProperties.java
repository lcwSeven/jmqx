package com.jmqx.store;

/**
 * Retained 存储参数。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class RetainedStoreProperties {
    private String rocksdbPath = "data/retained-rocksdb";
    private int maxEntries = 100_000;
    private long maxBytes = 256L * 1024 * 1024;
    private int maxPayloadBytes = 1024 * 1024;
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
}
