package org.archive.archivespark.sparkling.io

import java.io.{ByteArrayInputStream, InputStream}

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.archive.archivespark.sparkling.logging.LogContext
import org.archive.archivespark.sparkling.util.Common

import scala.util.Try

class HdfsBlockStream (fs: FileSystem, file: String, offset: Long = 0, length: Long = -1, retries: Int = 60, sleepMillis: Int = 1000, blockReadTimeoutMillis: Int = -1) extends InputStream {
  implicit val logContext: LogContext = LogContext(this)

  val path = new Path(file)
  val (blockSize: Int, fileSize: Long) = {
    val status = fs.getFileStatus(path)
    (status.getBlockSize.min(Int.MaxValue).toInt, status.getLen)
  }

  private val PeekBlockSize = 1024 * 1024 // 1MB

  private var peek: Boolean = true
  private var pos: Long = offset.max(0)
  private val max: Long = if (length > 0) fileSize.min(pos + length) else fileSize

  private var buffer = new Array[Byte](blockSize)
  private val emptyBlock = new ByteArrayInputStream(Array.emptyByteArray)
  private var block = emptyBlock

  def ensureNextBlock(): InputStream = {
    if (block.available == 0 && pos < max) {
      val end = pos + blockSize
      val nextBlockLength = ((end - (end % blockSize)).min(max) - pos).toInt
      val blockLength = if (peek) PeekBlockSize.min(nextBlockLength) else nextBlockLength
      peek = false
      Common.retryObj(fs.open(path, blockLength))(retries, sleepMillis, _.close, (_, retry, e) => {
        "File access failed (" + retry + "/" + retries + "): " + path + " (Offset: " + pos + ")" + Option(e.getMessage).map(_.trim).filter(_.nonEmpty).map(" - " + _).getOrElse("")
      }) { (in, retry) =>
        Common.timeout(blockReadTimeoutMillis, Some("Reading " + path + " (Offset: " + pos + ")")) {
          try {
            if (retry > 0) Try(in.seekToNewSource(pos))
            else if (pos > 0) in.seek(pos)
            var read = 0
            while (read < blockLength) read += in.read(buffer, read, blockLength - read)
          } finally {
            Try(in.close())
          }
        }
      }
      pos += blockLength
      block = new ByteArrayInputStream(buffer, 0, blockLength)
      System.gc()
    }
    block
  }

  override def read(): Int = ensureNextBlock().read()

  override def read(b: Array[Byte]): Int = ensureNextBlock().read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = ensureNextBlock().read(b, off, len)

  override def skip(n: Long): Long = {
    val available = block.available
    if (n <= available) block.skip(n)
    else {
      block = emptyBlock
      val currentPos = pos - available
      val skip = n.min(max - currentPos)
      pos += skip - available
      skip
    }
  }

  override def available(): Int = block.available

  override def close(): Unit = {
    block = emptyBlock
    buffer = Array.emptyByteArray
  }

  override def markSupported(): Boolean = false
}
