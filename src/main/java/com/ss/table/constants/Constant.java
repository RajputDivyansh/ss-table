package com.ss.table.constants;

public interface Constant {
    int COMPACTION_TRIGGER_FILE_COUNT = 3;
    long COMPACTION_TRIGGER_SIZE_BYTES = 50 * 1024 * 1024; // 50MB
}
