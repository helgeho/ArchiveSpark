package org.archive.archivespark.sparkling.io

import java.io.OutputStream

class NonClosingOutputStream(out: OutputStream) extends OutputStream {
  override def write(b: Int): Unit = out.write(b)
  override def write(b: Array[Byte]): Unit = out.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = out.write(b, off, len)
  override def flush(): Unit = out.flush()
  override def close(): Unit = {}
}
