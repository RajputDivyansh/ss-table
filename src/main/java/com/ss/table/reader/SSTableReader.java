package com.ss.table.reader;

import com.ss.table.filter.BloomFilter;
import com.ss.table.model.SSTableValueModel;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SSTableReader {
    private final Map<String, Integer> index = new HashMap<>();
    private BloomFilter bloomFilter;
    private RandomAccessFile file;
    private FileChannel channel;

    public SSTableReader(String filePath) throws IOException {
        this.file = new RandomAccessFile(filePath, "r");
        this.channel = file.getChannel();
        loadFooterAndStructure();
    }

    private void loadFooterAndStructure() throws IOException {
        channel.position(channel.size() - 12);
        ByteBuffer footerBuffer = ByteBuffer.allocate(12);
        channel.read(footerBuffer);
        footerBuffer.flip();

        int indexStart = footerBuffer.getInt();
        int bloomStart = footerBuffer.getInt();
        int bloomLength = footerBuffer.getInt();

        ByteBuffer bloomBuf = ByteBuffer.allocate(bloomLength);
        channel.position(bloomStart);
        channel.read(bloomBuf);
        bloomBuf.flip();

        byte[] bloomBytes = new byte[bloomLength];
        bloomBuf.get(bloomBytes);
        this.bloomFilter = new BloomFilter(1024);
        this.bloomFilter.loadFromBytes(bloomBytes);

        channel.position(indexStart);
        ByteBuffer indexBuf = ByteBuffer.allocate(bloomStart - indexStart);
        channel.read(indexBuf);
        indexBuf.flip();
        while (indexBuf.hasRemaining()) {
            int keyLen = indexBuf.getInt();
            byte[] keyBytes = new byte[keyLen];
            indexBuf.get(keyBytes);
            int offset = indexBuf.getInt();
            index.put(new String(keyBytes), offset);
        }
    }

    public SSTableValueModel get(String key) throws IOException {
        if (!bloomFilter.mightContain(key)) return null;
        Integer offset = index.get(key);
        if (offset == null) return null;

        channel.position(offset);

        ByteBuffer keyLenBuf = ByteBuffer.allocate(4);
        channel.read(keyLenBuf);
        keyLenBuf.flip();
        int keyLen = keyLenBuf.getInt();

        ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
        channel.read(keyBuf);
        keyBuf.flip();
        String foundKey = new String(keyBuf.array());
        if (!foundKey.equals(key)) return null;

        ByteBuffer valLenBuf = ByteBuffer.allocate(4);
        channel.read(valLenBuf);
        valLenBuf.flip();
        int valLen = valLenBuf.getInt();

        ByteBuffer valBuf = ByteBuffer.allocate(valLen);
        channel.read(valBuf);
        valBuf.flip();
        String value = new String(valBuf.array());

        ByteBuffer timestampBuf = ByteBuffer.allocate(8);
        channel.read(timestampBuf);
        timestampBuf.flip();
        long timestamp = timestampBuf.getLong();

        ByteBuffer tombstoneBuf = ByteBuffer.allocate(1);
        channel.read(tombstoneBuf);
        tombstoneBuf.flip();
        boolean isTombstone = tombstoneBuf.get() == 1;

        ByteBuffer ttlBuf = ByteBuffer.allocate(8);
        channel.read(ttlBuf);
        ttlBuf.flip();
        long ttlMillis = ttlBuf.getLong();

        return new SSTableValueModel(value, timestamp, isTombstone, ttlMillis);
    }

    public Set<String> getAllKeys() {
        return new HashSet<>(index.keySet());
    }
}
