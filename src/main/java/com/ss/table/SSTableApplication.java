package com.ss.table;

import com.ss.table.reader.SSTableReader;
import com.ss.table.writer.SSTableWriter;

import java.io.IOException;

public class SSTableApplication {
    public static void main( String[] args ) throws IOException {
        SSTableWriter writer = new SSTableWriter();
        writer.put("apple", "fruit");
        writer.put("banana", "yellow");
        writer.put("carrot", "vegetable");
        writer.writeToFile("sstable.data");
        System.out.println("✅ SSTable written to disk");

        SSTableReader reader = new SSTableReader("sstable.data");
        System.out.println("🔍 Get appl: " + reader.get("appl"));
        System.out.println("🔍 Get banana: " + reader.get("banana"));
        System.out.println("🔍 Get unknown: " + reader.get("unknown"));
    }
}
