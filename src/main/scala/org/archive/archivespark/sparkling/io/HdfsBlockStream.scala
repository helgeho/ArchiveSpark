/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.archive.archivespark.sparkling.io

import java.io.{ByteArrayInputStream, InputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.archive.archivespark.sparkling.logging.LogContext
import org.archive.archivespark.sparkling.util.Common

import scala.util.Try

class HdfsBlockStream (fs: FileSystem, file: String, offset: Long = 0, length: Long = -1, retries: Int = 60, sleepMillis: Int = 1000 * 60) extends InputStream {
  implicit val logContext: LogContext = LogContext(this)

  val path = new Path(file)
  val (blockSize: Int, fileSize: Long) = {
    val status = fs.getFileStatus(path)
    (status.getBlockSize.min(Int.MaxValue).toInt, status.getLen)
  }

  private var pos: Long = offset.max(0)
  private val max: Long = if (length > 0) fileSize.min(pos + length) else fileSize

  private val buffer = new Array[Byte](blockSize)
  private val emptyBlock = new ByteArrayInputStream(Array.emptyByteArray)
  private var block: ByteArrayInputStream = emptyBlock

  def ensureNextBlock(): InputStream = {
    if (block.available() == 0 && pos < max) {
      val end = pos + blockSize
      val blockLength = ((end - (end % blockSize)).min(max) - pos).toInt
      Common.retry(retries, sleepMillis, (retry, e) => {
        "File access failed (" + retry + "/" + retries + "): " + path + " (Offset: " + pos + ") - " + e.getMessage
      }) { retry =>
        val in = fs.open(path, blockLength)
        if (retry > 0) Try(in.seekToNewSource(pos))
        else if (pos > 0) in.seek(pos)
        var read = 0
        while (read < blockLength) read += in.read(buffer, read, blockLength - read)
        Try(in.close())
      }
      pos += blockLength
      block = new ByteArrayInputStream(buffer, 0, blockLength)
    }
    block
  }

  override def read(): Int = ensureNextBlock().read()

  override def read(b: Array[Byte]): Int = ensureNextBlock().read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = ensureNextBlock().read(b, off, len)

  override def skip(n: Long): Long = {
    val available = block.available()
    if (n <= available) block.skip(n)
    else {
      block = emptyBlock
      val currentPos = pos - available
      val skip = n.min(max - currentPos)
      pos += skip - available
      skip
    }
  }

  override def available(): Int = block.available()

  override def close(): Unit = {}
  override def markSupported(): Boolean = false
}
