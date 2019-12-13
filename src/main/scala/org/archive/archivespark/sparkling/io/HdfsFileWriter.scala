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

import java.io.{FileInputStream, FileOutputStream, OutputStream}

import org.apache.hadoop.fs.Path
import org.archive.archivespark.sparkling.logging.{Log, LogContext}

import scala.util.Try

class HdfsFileWriter private(filename: String, append: Boolean, replication: Short) extends OutputStream {
  implicit val logContext: LogContext = LogContext(this)

  private val file = IOUtil.tmpFile

  Log.info("Writing to temporary local file " + file.getCanonicalPath + " (" + filename + ")...")

  val out = new FileOutputStream(file)

  override def close(): Unit = {
    Try { out.close() }
    Log.info("Copying from temporary file " + file.getCanonicalPath + " to " + filename + "...")
    if (append) {
      val in = new FileInputStream(file)
      val p = new Path(filename)
      val appendOut = HdfsIO.fs(p).append(p)
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
