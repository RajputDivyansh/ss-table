package com.ss.table.writer;

import com.ss.table.filter.BloomFilter;
import com.ss.table.model.SSTableValueModel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class SSTableWriter {

    private final TreeMap<String, SSTableValueModel> memtable = new TreeMap<>();
    private final BloomFilter bloomFilter = new BloomFilter(1024);

    public void put(String key, String value) {
        put(key, value, System.currentTimeMillis());
        bloomFilter.add(key);
    }

    public void put(String key, String value, long timeStamp) {
        memtable.put(key, new SSTableValueModel(value, timeStamp, false));
        bloomFilter.add(key);
    }

    public void delete(String key, long timestamp) {
        memtable.put(key, new SSTableValueModel("", timestamp, true));
        bloomFilter.add(key);
    }

    public void writeToFile(String filePath) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            FileChannel channel = file.getChannel();
            Map<String, Integer> index = new LinkedHashMap<>();
            ByteArrayOutputStream dataBlock = new ByteArrayOutputStream();

            for (Map.Entry<String, SSTableValueModel> entry : memtable.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes();
                byte[] valBytes = entry.getValue().getValue().getBytes();
                long timestamp = entry.getValue().getTimestamp();
                boolean isTombstone = entry.getValue().getTombstone();

                index.put(entry.getKey(), dataBlock.size());

                dataBlock.write(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
                dataBlock.write(keyBytes);
                dataBlock.write(ByteBuffer.allocate(4).putInt(valBytes.length).array());
                dataBlock.write(valBytes);
                dataBlock.write(ByteBuffer.allocate(8).putLong(timestamp).array());
                dataBlock.write((byte) (isTombstone ? 1 : 0));
            }

            long dataStart = channel.position();
            channel.write(ByteBuffer.wrap(dataBlock.toByteArray()));

            ByteArrayOutputStream indexBlock = new ByteArrayOutputStream();
            for (Map.Entry<String, Integer> idx : index.entrySet()) {
                byte[] keyBytes = idx.getKey().getBytes();
                indexBlock.write(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
                indexBlock.write(keyBytes);
                indexBlock.write(ByteBuffer.allocate(4).putInt(idx.getValue()).array());
            }

            long indexStart = channel.position();
            channel.write(ByteBuffer.wrap(indexBlock.toByteArray()));

            long bloomStart = channel.position();
            byte[] bloomData = bloomFilter.toBytes();
            channel.write(ByteBuffer.wrap(bloomData));

            ByteBuffer footer = ByteBuffer.allocate(12);
            footer.putInt((int) indexStart);
            footer.putInt((int) bloomStart);
            footer.putInt(bloomData.length);
            channel.write(ByteBuffer.wrap(footer.array()));

            channel.close();
        }
    }
}
