package com.ss.table.compactor;

import com.ss.table.model.SSTableValueModel;
import com.ss.table.reader.SSTableReader;
import com.ss.table.writer.SSTableWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class SSTableCompactor {

    public void compact(List<String> inputPaths, String outputPath, long gcGraceMillis)
          throws IOException {
        Map<String, SSTableValueModel> mergedData = getMergedSSTableValueModelMap(inputPaths);

        SSTableWriter writer = new SSTableWriter();
        for (Map.Entry<String, SSTableValueModel> entry : mergedData.entrySet()) {
            SSTableValueModel value = entry.getValue();

            if (value.getTtlMillis() > 0
                  && (System.currentTimeMillis() - value.getTimestamp()) > value.getTtlMillis()) {
                System.out.println("⏱️ TTL expired -> purging key: " + entry.getKey());
                continue;
            }

            if (value.getTombstone()) {
                if ((System.currentTimeMillis() - value.getTimestamp()) > gcGraceMillis) {
                    continue;
                }
                writer.delete(entry.getKey(), value.getTimestamp());
            } else {
                writer.put(entry.getKey(), value.getValue(), value.getTimestamp());
            }
        }
        writer.writeToFile(outputPath);
    }

    private Map<String, SSTableValueModel> getMergedSSTableValueModelMap(
          List<String> inputPaths) throws IOException {
        Map<String, SSTableValueModel> mergedData = new TreeMap<>();

        for (String path : inputPaths) {
            SSTableReader reader = new SSTableReader(path);
            Set<String> keys = reader.getAllKeys();
            for (String key : keys) {
                SSTableValueModel value = reader.get(key);
                if (value == null) continue;

                SSTableValueModel existing = mergedData.get(key);
                if (existing == null || value.getTimestamp() > existing.getTimestamp()) {
                    mergedData.put(key, value);
                }
            }
        }
        return mergedData;
    }
}
