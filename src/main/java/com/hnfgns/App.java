package com.hnfgns;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.misc.VM;

/**
 * Direct memory test.
 */
public class App {
  private final static Logger logger = LoggerFactory.getLogger(App.class);

  final static int DEFAULT_CHUNK_SIZE = 16 * 1024 * 1024; // 16M
  final static int DEFAULT_NUM_CHUNKS = 100;
  final static int DEFAULT_FREE_FROM = 5;
  final static int PRE_ALLOC_SLEEP = 20 * 1000; // 20 secs
  final static int POST_ALLOC_SLEEP = 20 * 1000; // 20 secs
  final static int POST_FREE_SLEEP = 20 * 1000; // 20 secs

  public static void main(String[] args)  throws Exception {
    int numChunks;
    int chunkSize;
    int freeFrom;
    int freeUntil;

    try {
      numChunks = Integer.valueOf(args[1]);
      chunkSize = Integer.valueOf(args[2]);
      freeFrom = Integer.valueOf(args[3]);
      freeUntil = Integer.valueOf(args[4]);
    } catch (Exception e) {
      logger.error("error while parsing params. using defaults", e);
      numChunks = DEFAULT_NUM_CHUNKS;
      chunkSize = DEFAULT_CHUNK_SIZE;
      freeFrom = DEFAULT_FREE_FROM;
      freeUntil = numChunks;
    }
    logger.info("numChunks: {} chunkSize: {} freeFrom: {} freeUntil: {}", numChunks, chunkSize, freeFrom, freeUntil);

    for (;;) {
      final long maxDirectMemory = VM.maxDirectMemory();
      logger.info("Max direct memory is {}", maxDirectMemory);

      final int totalDirectMemoryRequired = numChunks * chunkSize;
      if (totalDirectMemoryRequired > maxDirectMemory) {
        logger.warn("Increase direct memory at least {} bytes", totalDirectMemoryRequired - maxDirectMemory);
      }

      Thread.sleep(PRE_ALLOC_SLEEP);

      {
        logger.info("Allocating {} chunks of size {} netting {} bytes", numChunks, chunkSize, numChunks * chunkSize);
        final ByteBuffer[] buffers = allocateChunks(numChunks, chunkSize);
        Thread.sleep(POST_ALLOC_SLEEP);
        logger.info("De-allocating {} chunks from {}", Math.max(buffers.length - freeFrom, 0), freeFrom);
        for (int i = freeFrom; i < freeUntil; i++) {
          free(buffers[i]);
        }
      }

      Thread.sleep(POST_FREE_SLEEP);
    }
  }

  private static void free(ByteBuffer buffer) throws Exception {
    final Field cleanerField = buffer.getClass().getDeclaredField("cleaner");
    cleanerField.setAccessible(true);
    final Cleaner cleaner = Cleaner.class.cast(cleanerField.get(buffer));
    cleaner.clean();
  }


  private static ByteBuffer[] allocateChunks(final int numChunks, final int chunkSize) {
    final ByteBuffer[] buffers = new ByteBuffer[numChunks];
    for (int i=0; i<numChunks;i++) {
      buffers[i] = ByteBuffer.allocateDirect(chunkSize);
    }
    return buffers;
  }
}
