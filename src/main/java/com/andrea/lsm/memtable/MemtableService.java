package com.andrea.lsm.memtable;

import com.andrea.lsm.manifest.Manifest;
import com.andrea.lsm.sstable.SSTable;
import java.io.IOException;
import java.nio.file.Path;
import util.Constants;
import util.WAL;

public class MemtableService {
  private final Manifest manifest;
  private Memtable activeMemtable;
  private WAL wal;

  public MemtableService(Manifest manifest) throws IOException {
    this.manifest = manifest;
    this.activeMemtable = new Memtable();
    Path pathOfWal = Path.of(manifest.getFileDataDir(), Constants.WAL_FILENAME);
    util.WAL.recoverMemtableFromWal(pathOfWal, activeMemtable);
    this.wal = new WAL(pathOfWal);
  }

  public void put(String key, String value) throws IOException {
    wal.writeEntry(key, value);
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

    this.wal.delete();
    this.wal = new WAL(Path.of(manifest.getFileDataDir(), Constants.WAL_FILENAME));
  }

  public String get(String key) {
    return this.activeMemtable.get(key);
  }

  public void close() throws IOException {
    flush();
    wal.close();
  }
}
