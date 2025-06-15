package com.ss.table.model;

public class SSTableValueModel {
    final String value;
    final long timestamp;
    final boolean isTombstone;
    final long ttlMillis;

    public SSTableValueModel(String value, long timestamp, boolean isTombstone, long ttlMillis) {
        this.value = value;
        this.timestamp = timestamp;
        this.isTombstone = isTombstone;
        this.ttlMillis = ttlMillis;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean getTombstone() {
        return isTombstone;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public boolean isExpired(long now) {
        return ttlMillis > 0 && (now - timestamp) > ttlMillis;
    }

    @Override
    public String toString() {
        return value + " (ts=" + timestamp + ", tombstone=" + isTombstone + ", ttl=" + ttlMillis + ")";
    }

}

