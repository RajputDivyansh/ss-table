package com.ss.table;

import com.ss.table.compactor.SSTableCompactor;
import com.ss.table.manager.SSTableManager;
import com.ss.table.model.SSTableValueModel;
import com.ss.table.reader.SSTableReader;
import com.ss.table.writer.SSTableWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SSTableApplication {
    public static void main( String[] args ) throws IOException, InterruptedException {

        // remove this code when project is completed
        File file = new File("data/");
        if (file.isDirectory()) {
            Arrays.stream(Objects.requireNonNull(file.listFiles(
                  (dir, name) -> name.endsWith(".data")))).forEach(File::delete);
        }

//        SSTableWriter writer = new SSTableWriter();
//        writer.put("apple", "fruit");
//        writer.put("banana", "yellow");
//        writer.put("carrot", "vegetable");
//        writer.delete("banana", System.currentTimeMillis());
//        writer.writeToFile("sstable.data");
//        System.out.println("✅ SSTable written to disk");
//
//        SSTableReader reader = new SSTableReader("sstable.data");
//        SSTableValueModel res = reader.get("banana");
//        System.out.println("banana => " + (res == null ? "null" : res.getValue() + " (deleted: " + res.getTombstone() + ")"));
//        SSTableValueModel res1 = reader.get("carrot");
//        System.out.println("banana => " + (res1 == null ? "null" : res1.getValue() + " (deleted: " + res1.getTombstone() + ")"));


        // CUSTOM LOGIC TO WRITE TO MULTIPLE SS TABLE AND THEN MERGE THEM

        // Write multiple SSTables
//        SSTableWriter writer1 = new SSTableWriter();
//        writer1.put("apple", "fruit");
//        writer1.put("banana", "yellow");
//        writer1.delete("carrot", System.currentTimeMillis());     // Mark banana deleted
//        writer1.writeToFile("data/sstable1.data");
//
//        Thread.sleep(100); // Ensure newer timestamps
//
//        SSTableWriter writer2 = new SSTableWriter();
//        writer2.put("apple", "red"); // Newer update
//        writer2.delete("banana", System.currentTimeMillis());     // Mark banana deleted
//        writer2.put("carrot", "vegetable");
//        writer2.writeToFile("data/sstable2.data");
//
//        Thread.sleep(100); // Ensure newer timestamps
//
//        List<String> sstables = List.of("data/sstable1.data", "data/sstable2.data");
//        String output = "data/merged-sstable.data";
//
//        SSTableCompactor compactor = new SSTableCompactor();
//        compactor.compact(sstables, output);
//
//        System.out.println("✅ Compaction complete -> " + new File(output).getAbsolutePath());
//
//        // Read merged result
//        SSTableReader reader = new SSTableReader(output);
//        for (String key : List.of("apple", "banana", "carrot")) {
//            SSTableValueModel val = reader.get(key);
//            System.out.println(key + " => " + (val == null ? "null" : val.getValue() + " (deleted: " + val.getTombstone() + ")"));
//        }

        // Initialize manager with:
        // - data directory = "data/"
        // - maxSSTablesBeforeCompact = 10
        // - maxTotalSizeBeforeCompactBytes = 200 bytes
        SSTableManager manager = new SSTableManager("data/", 10, 200);

        // Simulate writes across time
        for (int i = 0; i < 5; i++) {
            int index = i;
            manager.writeSSTable(dir -> {
                SSTableWriter writer = new SSTableWriter();

                // Example with TTL of 3 seconds for some keys
                long ttl = (index % 2 == 0) ? 100 : 0;
                writer.put("key" + index, "val" + index, System.currentTimeMillis(), ttl);

                // Write to a uniquely named file in the directory
                String filePath = new File(dir, "sstable-" + System.currentTimeMillis() + ".data").getAbsolutePath();
                writer.writeToFile(filePath);
                return filePath;
            });

            Thread.sleep(2000); // Simulate staggered writes
        }

        // Wait to allow background compaction and TTL expiry
        Thread.sleep(12000);

        // Shutdown background compactor thread
        manager.shutdown();

        System.out.println("✅ SSTable writes and background compaction completed.");
        System.out.println("Below are the available content of SSTable: ");
        SSTableReader finalReader =
              new SSTableReader(new File("data/").listFiles()[0].getAbsolutePath());
        for (String key : finalReader.getAllKeys()) {
            SSTableValueModel val = finalReader.get(key);
            System.out.println(key + " => " + val);
        }
    }
}
