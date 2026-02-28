package com.andrea.lsm.memtable;

import com.andrea.lsm.manifest.Manifest;
import com.andrea.lsm.sstable.SSTableService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import util.Constants;
import util.WAL;

public class MemtableService {
  private final Manifest manifest;
  private final SSTableService sstableService;
  private Memtable activeMemtable;
  private WAL activeWal;
  private final Path rootPath;

  public MemtableService(Manifest manifest, SSTableService sstableService) throws IOException {
    this.manifest = manifest;
    this.sstableService = sstableService;
    this.activeMemtable = new Memtable();
    this.rootPath = manifest.getRootPath();

    util.WAL.recoverAll(rootPath, activeMemtable);
    if (activeMemtable.getSize() > 0) {
      sstableService.flush(activeMemtable);
      cleanOldWals();
      this.activeMemtable = new Memtable();
    }

    this.activeWal = new WAL(WAL.generateWALPath(rootPath));
  }

  public void put(String key, String value) throws IOException {
    activeWal.writeEntry(key, value);
    this.activeMemtable.put(key, value);
    if (activeMemtable.getSize() > Constants.MAXSIZE_MEMTABLE) {
      rotateAndFlush();
    }
  }

  private synchronized void rotateAndFlush() throws IOException {
      Memtable memtableToFlush = this.activeMemtable;
      this.activeMemtable = new Memtable();
      WAL fullWal = this.activeWal;
      this.activeWal = new WAL(manifest.getRootPath().resolve(
          Constants.WAL_PREFIX + System.nanoTime() + Constants.WAL_FILE_EXTENSION));
      try {
        sstableService.flush(memtableToFlush);
        fullWal.delete();
      } catch (Exception e) {
        System.err.println("Flush failed! Data saved in WAL but not SSTable.");
        throw e;
      }
  }

  public String get(String key) {
    return this.activeMemtable.get(key);
  }

  private void cleanOldWals() throws IOException {
    try (Stream<Path> files = Files.list(rootPath)) {
      files.filter(path -> path.getFileName().toString().startsWith(Constants.WAL_PREFIX))
          .filter(path -> path.getFileName().toString().endsWith(Constants.WAL_FILE_EXTENSION))
          .forEach(path -> {
            try {
              Files.deleteIfExists(path);
            } catch (IOException e) {
              System.err.println("Failed to delete old WAL: " + path);
            }
          });
    }
  }

  public void close() throws IOException {
    sstableService.flush(activeMemtable);
    activeWal.close();
  }
}
