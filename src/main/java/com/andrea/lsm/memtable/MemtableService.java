package com.andrea.lsm.memtable;

import com.andrea.lsm.manifest.Manifest;
import com.andrea.lsm.sstable.SSTable;
import java.io.IOException;
import java.nio.file.Path;
import util.Constants;

public class MemtableService {
  private final Manifest manifest;
  private Memtable activeMemtable;

  public MemtableService(Manifest manifest) {
    this.manifest = manifest;
    this.activeMemtable = new Memtable();
  }

  public void put(String key, String value) throws IOException {
    this.activeMemtable.put(key, value);
    if (activeMemtable.getSize() > Constants.MAXSIZE_MEMTABLE)  {
      flush();
    }
  }
  private void flush() throws IOException {
    if (activeMemtable.getSize() == 0) {return;}
    Path path = Path.of(manifest.getFileDataDir());
    SSTable sstable = SSTable.createSSTableFromMemtable(activeMemtable, path);
    this.manifest.addSSTable(0, sstable);
    this.activeMemtable = new Memtable();
  }

  public String get(String key) {
    return this.activeMemtable.get(key);
  }

  public void close() throws IOException {
    flush();
  }
}
