package com.ss.table.filter;

import java.util.BitSet;
import java.util.zip.CRC32;

public class BloomFilter {

    private final BitSet bits;
    private final int size;

    public BloomFilter(int size) {
        this.size = size;
        this.bits = new BitSet(size);
    }

    public void add(String key) {
        int h1 = safeAbs(hash(key, 17)) % size;
        int h2 = safeAbs(hash(key, 31)) % size;
        bits.set(h1);
        bits.set(h2);
    }

    public boolean mightContain(String key) {
        int h1 = safeAbs(hash(key, 17)) % size;
        int h2 = safeAbs(hash(key, 31)) % size;
        return bits.get(h1) && bits.get(h2);
    }

    private int safeAbs(int hash) {
        return hash == Integer.MIN_VALUE ? 0 : Math.abs(hash);
    }

    private int hash(String s, int seed) {
        CRC32 crc = new CRC32();
        crc.update((s + seed).getBytes());
        return (int) crc.getValue();
    }

    public byte[] toBytes() {
        return bits.toByteArray();
    }

    public void loadFromBytes(byte[] data) {
        bits.clear();
        for (int i = 0; i < data.length * 8; i++) {
            if ((data[i / 8] & (1 << (i % 8))) != 0) {
                bits.set(i);
            }
        }
    }
}
