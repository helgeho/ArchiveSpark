package org.archive.archivespark.sparkling.io

import java.io.InputStream

import org.archive.archivespark.sparkling.util.Common

class CleanupInputStream(in: InputStream, cleanup: () => Unit) extends InputStream {
  override def read(): Int = in.read()
  override def read(b: Array[Byte]): Int = in.read(b)
  override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)
  override def skip(n: Long): Long = in.skip(n)
  override def available(): Int = in.available()
  override def close(): Unit = Common.cleanup(in.close())(cleanup)
  override def mark(readlimit: Int): Unit = in.mark(readlimit)
  override def reset(): Unit = in.reset()
  override def markSupported(): Boolean = in.markSupported()
}
