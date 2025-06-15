package com.ss.table.manager;

import com.ss.table.compactor.SSTableCompactor;
import com.ss.table.constants.Constant;
import com.ss.table.writer.SSTableWriterRunnable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SSTableManager {

    private final File ssTableDir;
    private final SSTableCompactor compactor = new SSTableCompactor();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final long gcGraceMillis;
    private final long backgroundCompactionIntervalMillis;

    public SSTableManager(String dirPath, long gcGraceMillis,
          long backgroundCompactionIntervalMillis) {
        this.gcGraceMillis = gcGraceMillis;
        this.backgroundCompactionIntervalMillis = backgroundCompactionIntervalMillis;
        this.ssTableDir = new File(dirPath);
        if (!ssTableDir.exists()) {
            ssTableDir.mkdir();
        }
        // starting background compaction
        startBackGroundCompaction();
    }

    private void startBackGroundCompaction() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("🕒 [Background] Checking compaction conditions...");
                File[] ssTableFiles = listSSTableFiles();
                if (ssTableFiles.length >= 2) {
                    System.out.println("🧹 Triggering background compaction");
                    compactEligible(ssTableFiles);
                }
            } catch (Exception e) {
                System.err.println("⚠️ Background compaction error: " + e.getMessage());
                e.printStackTrace();
            }
        }, backgroundCompactionIntervalMillis, backgroundCompactionIntervalMillis, TimeUnit.MILLISECONDS);
    }

    public void writeSSTable(SSTableWriterRunnable writerRunnable) throws IOException {
        String path = writerRunnable.writeToDir(ssTableDir);
        System.out.println("📝 SSTable written: " + path);
        maybeCompact();
    }

    private void maybeCompact() throws IOException {
        File[] ssTableFiles = ssTableDir.listFiles((dir, name) -> name.endsWith(".data"));
        if (ssTableFiles == null || ssTableFiles.length == 0) return;

        int fileCount = ssTableFiles.length;
        long totalSize = Arrays.stream(ssTableFiles).mapToLong(File::length).sum();

        if (Constant.COMPACTION_TRIGGER_FILE_COUNT <= fileCount
              || Constant.COMPACTION_TRIGGER_SIZE_BYTES <= totalSize) {
            compactEligible(ssTableFiles);
        }
    }

    private void compactEligible(File[] ssTableFiles) throws IOException {
        List<String> filePaths = new ArrayList<>();
        for (File file : ssTableFiles) {
            filePaths.add(file.getAbsolutePath());
        }
        if (filePaths.size() < 2) return; // Need at least 2 to compact

        String output = new File(ssTableDir,
              "merged-" + System.currentTimeMillis() + ".data").getAbsolutePath();
        System.out.println("🧹 Triggering compaction → " + output);

        compactor.compact(filePaths, output, gcGraceMillis);

        for (File file : ssTableFiles) {
            file.delete();
        }
        System.out.println("✅ Compaction complete. Old files removed.");
    }

    private File[] listSSTableFiles() {
        File[] files = ssTableDir.listFiles((dir, name) -> name.endsWith(".data"));
        return files != null ? files : new File[0];
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
