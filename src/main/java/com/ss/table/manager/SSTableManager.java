package com.ss.table.manager;

import com.ss.table.compactor.SSTableCompactor;
import com.ss.table.constants.Constant;
import com.ss.table.writer.SSTableWriter;
import com.ss.table.writer.SSTableWriterRunnable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SSTableManager {

    private final File ssTableDir;
    private final SSTableCompactor compactor = new SSTableCompactor();

    public SSTableManager(String dirPath) {
        this.ssTableDir = new File(dirPath);
        if (!ssTableDir.exists()) {
            ssTableDir.mkdir();
        }
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
            List<String> filePaths = new ArrayList<>();
            for (File file : ssTableFiles) {
                filePaths.add(file.getAbsolutePath());
            }

            String output = new File(ssTableDir,
                  "merged-" + System.currentTimeMillis() + ".data").getAbsolutePath();
            System.out.println("🧹 Triggering compaction → " + output);

            compactor.compact(filePaths, output);

            for (File file : ssTableFiles) {
                file.delete();
            }
            System.out.println("✅ Compaction complete. Old files removed.");
        }
    }
}
