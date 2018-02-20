/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2018 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.dataspecs.access

import java.io.InputStream
import java.util.zip.GZIPInputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil

class HdfsFileAccessor(path: String, decompress: Boolean = true) extends CloseableDataAccessor[InputStream] {
  override def get: Option[InputStream] = {
    val fs = FileSystem.get(SparkHadoopUtil.get.conf)
    var stream: InputStream = null
    try {
      val raw = fs.open(new Path(path))
      stream = if (decompress) HdfsFileAccessor.decompress(path, raw) else raw
      Some(stream)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        if (stream != null) stream.close()
        None
    }
  }
}

object HdfsFileAccessor {
  def decompress(path: String, stream: InputStream): InputStream = {
    if (path.toLowerCase.endsWith(".gz")) new GZIPInputStream(stream)
    else stream
  }
}