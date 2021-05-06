package org.archive.archivespark.sparkling.io

import java.io.{FileInputStream, OutputStream}

import org.apache.hadoop.fs.Path
import org.archive.archivespark.sparkling.logging.{Log, LogContext}

import scala.util.Try

class HdfsFileWriter private(filename: String, append: Boolean, replication: Short) extends OutputStream {
  implicit val logContext: LogContext = LogContext(this)

  private val file = IOUtil.tmpFile

  Log.info("Writing to temporary local file " + file.getCanonicalPath + " (" + filename + ")...")

  val out = IOUtil.fileOut(file)

  override def close(): Unit = {
    Try { out.close() }
    Log.info("Copying from temporary file " + file.getCanonicalPath + " to " + filename + "...")
    if (append) {
      val in = new FileInputStream(file)
      val appendOut = HdfsIO.fs.append(new Path(filename))
      IOUtil.copy(in, appendOut)
      appendOut.close()
      in.close()
      file.delete()
    } else HdfsIO.copyFromLocal(file.getCanonicalPath, filename, move = true, overwrite = true, replication)
    Log.info("Done. (" + filename + ")")
  }

  override def write(b: Int): Unit = out.write(b)
  override def write(b: Array[Byte]): Unit = out.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = out.write(b, off, len)
  override def flush(): Unit = out.flush()
}

object HdfsFileWriter {
  def apply(filename: String, overwrite: Boolean = false, append: Boolean = false, replication: Short = 0): HdfsFileWriter = {
    if (!overwrite && !append) HdfsIO.ensureNewFile(filename)
    new HdfsFileWriter(filename, append, replication)
  }
}
