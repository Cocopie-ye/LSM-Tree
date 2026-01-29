package com.andrea.lsm.memtable;

import com.andrea.lsm.manifest.Manifest;
import com.andrea.lsm.sstable.SSTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import util.Constants;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MemtableService}, validating the lifecycle of
 * in-memory data, including automatic and manual flushing to persistent storage.
 */
class MemtableServiceTest {

  @TempDir
  Path tempDir; // JUnit 5 automatically manages this lifecycle-bound temporary directory

  private MemtableService memtableService;
  private Manifest manifest;

  @BeforeEach
  void setUp() throws IOException {
    // 1. Initialize Manifest within the temporary directory for metadata tracking
    manifest = new Manifest(tempDir.toString());

    // 2. Initialize the service with the localized manifest
    memtableService = new MemtableService(manifest);
  }

  @AfterEach
  void tearDown() {
    // Cleanup is handled automatically by the @TempDir extension
  }

  @Test
  void testPutAndGetInMemory() throws IOException {
    // Validate basic volatile storage operations (Read-Your-Writes consistency)
    memtableService.put("key1", "value1");
    String val = memtableService.get("key1");

    assertEquals("value1", val, "Data should be retrievable from the Active Memtable");

    // Ensure no premature persistence has occurred
    assertEquals(0, manifest.getSSTable(0).size(),
        "Data should not be flushed to disk before reaching the size threshold");
  }

  @Test
  void testFlushOnThreshold() throws IOException {
    // Objective: Trigger an automatic flush by exceeding the Memtable size limit
    int threshold = Constants.MAXSIZE_MEMTABLE;

    // Generate a payload that exceeds the threshold (Key size + Value size > threshold)
    String bigValue = "x".repeat(threshold);

    // 1. Write the large payload to trigger an immediate background/synchronous flush
    memtableService.put("bigKey", bigValue);

    // 2. Verify that the Manifest has registered the new L0 SSTable
    List<SSTable> tables = manifest.getSSTable(0);
    assertEquals(1, tables.size(), "Automatic flush should occur when the size threshold is breached");

    // 3. Verify Memtable reset
    // Once flushed, the active Memtable is rotated; older data now resides in persistent storage
    assertNull(memtableService.get("bigKey"), "The active Memtable should be cleared post-flush");

    // 4. Validate file placement
    // Ensure the SSTable is physically located within the managed environment
    SSTable generatedSSTable = tables.get(0);
    String filePath = String.valueOf(generatedSSTable.getFilePath());
    assertTrue(filePath.startsWith(tempDir.toString()),
        "The SSTable file must be stored within the Manifest-designated directory");
  }

  @Test
  void testCloseTriggersFlush() throws IOException {
    // 1. Perform a small write that does not trigger the threshold
    memtableService.put("k_close", "v_close");

    // Verify data remains exclusively in memory
    assertEquals(0, manifest.getSSTable(0).size());

    // 2. Perform a graceful shutdown of the service
    memtableService.close();

    // 3. Validation
    List<SSTable> tables = manifest.getSSTable(0);
    assertEquals(1, tables.size(), "Service closure must trigger a mandatory flush to prevent data loss");

    // Verify data integrity in the resulting SSTable
    SSTable sst = tables.get(0);
    assertEquals("v_close", sst.get("k_close"), "Flushed data must maintain integrity and be readable from disk");
  }
}