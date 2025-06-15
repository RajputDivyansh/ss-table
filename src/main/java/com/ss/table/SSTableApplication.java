package com.ss.table;

import com.ss.table.compactor.SSTableCompactor;
import com.ss.table.manager.SSTableManager;
import com.ss.table.model.SSTableValueModel;
import com.ss.table.reader.SSTableReader;
import com.ss.table.writer.SSTableWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SSTableApplication {
    public static void main( String[] args ) throws IOException, InterruptedException {

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

        SSTableManager manager = new SSTableManager("data/");

        for (int i = 0; i < 5; i++) {
            int index = i;
            manager.writeSSTable(dir -> {
                SSTableWriter writer = new SSTableWriter();
                writer.put("key" + index, "val" + index);
                String filePath = new File(dir, "sstable-" + System.currentTimeMillis() + ".data").getAbsolutePath();
                writer.writeToFile(filePath);
                return filePath;
            });
            Thread.sleep(2000); // Simulate time between writes
        }
    }
}
