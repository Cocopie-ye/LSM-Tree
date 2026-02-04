package util;

import com.andrea.lsm.memtable.Memtable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WAL implements AutoCloseable{
  private Path filePath;
  private DataOutputStream out;

  public WAL(Path filePath) throws IOException {
    this.filePath = filePath;
    this.out = new DataOutputStream(
        new BufferedOutputStream(Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)));
  }

  public void writeEntry(String key, String value) throws IOException {
    // append : KeyLen(4) + Key + ValLen(4) + Value
    byte[] keyBytes = key.getBytes();
    byte[] valueBytes = value.getBytes();
    out.writeInt(keyBytes.length);
    out.write(keyBytes);
    out.writeInt(valueBytes.length);
    out.write(valueBytes);

    out.flush();
  }

  public void delete() throws IOException {
    this.close();
    Files.deleteIfExists(filePath);
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  public static void recoverMemtableFromWal(Path walPath, Memtable memtable) throws IOException {
    if (!Files.exists(walPath)) {return;}

    try (DataInputStream in = new DataInputStream(
        new BufferedInputStream(Files.newInputStream(walPath)))) {
      while (in.available() > 0) {
        int keyLen = in.readInt();
        byte[] keyBytes = new byte[keyLen];
        in.readFully(keyBytes);
        int valueLen = in.readInt();
        byte[] valueBytes = new byte[valueLen];
        in.readFully(valueBytes);

        memtable.put(new String(keyBytes), new String(valueBytes));
      }
    } catch (IOException e) {
      System.err.println("Warning: WAL file ended unexpectedly (truncated). Recovered data up to the break.");
    }
  }
}
