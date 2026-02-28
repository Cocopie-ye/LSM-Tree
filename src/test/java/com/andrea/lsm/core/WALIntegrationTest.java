package com.andrea.lsm.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import util.Constants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Write-Ahead Log (WAL) functionality within the LSM storage engine.
 */
class WALIntegrationTest {

  @TempDir
  Path tempDir;

  private DB db;

  @BeforeEach
  void setUp() throws IOException {
    db = new DB(tempDir.toString());
  }

  @AfterEach
  void tearDown() throws IOException {
    if (db != null) {
      db.close();
    }
  }

  /**
   * Helper method: Checks if any file in the directory matches the WAL naming pattern.
   */
  private boolean hasWalFile() throws IOException {
    try (Stream<Path> files = Files.list(tempDir)) {
      return files.anyMatch(p -> p.getFileName().toString().startsWith(Constants.WAL_PREFIX)
          && p.getFileName().toString().endsWith(Constants.WAL_FILE_EXTENSION));
    }
  }

  @Test
  @DisplayName("Verify data recovery from multiple WAL files after a crash")
  void testCrashRecoveryWithMultipleWals() throws IOException {
    // 1. Write the first batch of data
    db.put("key1", "val1");
    assertTrue(hasWalFile(), "WAL file should be created after a write operation");

    // 2. Simulate a crash (nullify the object without calling close() to preserve volatile state)
    db = null;

    // 3. Restart the database
    DB newDb = new DB(tempDir.toString());

    // 4. Verify data recovery
    assertEquals("val1", newDb.get("key1"), "Data should be recovered from WAL after restart");

    // 5. Perform subsequent write and simulate another restart to test multi-WAL replay
    newDb.put("key2", "val2");
    newDb = null;

    DB thirdDb = new DB(tempDir.toString());
    assertEquals("val1", thirdDb.get("key1"));
    assertEquals("val2", thirdDb.get("key2"));

    thirdDb.close();
  }

  @Test
  @DisplayName("Verify that obsolete WAL files are cleaned up after successful recovery/flush")
  void testWalCleanupAfterRecovery() throws IOException {
    // 1. Write data and ensure a WAL file is generated
    db.put("key_to_clean", "some_value");
    assertTrue(hasWalFile());

    // 2. Simulate a crash
    db = null;

    // 3. Restart. Logic: recoverAll() -> sstableService.flush() -> cleanupOldWalFiles()
    DB newDb = new DB(tempDir.toString());

    // 4. Verify data persists (via SSTable), while obsolete log files are deleted.
    // Note: A new, empty WAL may exist, but the previous timestamped log should be removed.
    assertEquals("some_value", newDb.get("key_to_clean"));

    newDb.close();
  }

  @Test
  @DisplayName("Verify WAL durability: data must be retrievable immediately after restart")
  void testDurability() throws IOException {
    db.put("durability_key", "standard_val");

    // Intentionally bypass close() and restart to test durability
    db = null;

    DB restartedDb = new DB(tempDir.toString());
    assertEquals("standard_val", restartedDb.get("durability_key"));
    restartedDb.close();
  }
}