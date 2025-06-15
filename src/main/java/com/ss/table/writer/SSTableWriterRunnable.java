package com.ss.table.writer;

import java.io.File;
import java.io.IOException;

public interface SSTableWriterRunnable {

    String writeToDir(File dir) throws IOException;
}
