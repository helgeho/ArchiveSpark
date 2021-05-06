package org.archive.archivespark.sparkling.io

import java.io.{File, OutputStream}

class AppendingFileOutputStream(val file: File, onOpen: AppendingFileOutputStream => Unit = _ => {}, onClose: AppendingFileOutputStream => Unit = _ => {}) extends OutputStream {
  private var out: Option[OutputStream] = None

  private def stream: OutputStream = {
    out match {
      case Some(stream) => stream
      case None =>
        val stream = IOUtil.fileOut(file, true)
        out = Some(stream)
        onOpen(this)
        stream
    }
  }

  override def write(b: Int): Unit = stream.write(b)
  override def write(b: Array[Byte]): Unit = stream.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = stream.write(b, off, len)

  override def flush(): Unit = for (stream <- out) stream.flush()

  override def close(): Unit = for (stream <- out) {
    onClose(this)
    out = None
    stream.close()
  }
}
