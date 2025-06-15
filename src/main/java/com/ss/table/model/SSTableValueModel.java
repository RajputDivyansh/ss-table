package com.ss.table.model;

public class SSTableValueModel {
    final String value;
    final long timestamp;
    final boolean isTombstone;

    public SSTableValueModel(String value, long timestamp, boolean isTombstone) {
        this.value = value;
        this.timestamp = timestamp;
        this.isTombstone = isTombstone;
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

    @Override
    public String toString() {
        return value + " (ts=" + timestamp + ", tombstone=" + isTombstone + ")";
    }

}

