/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2024 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.dataspecs.access

import java.io.InputStream

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil

class HdfsStreamAccessor(location: HdfsLocationInfo) extends CloseableDataAccessor[InputStream] {
  override def get: Option[InputStream] = {
    if (location.length < 0 || location.offset < 0) None
    else {
      val fs = FileSystem.get(SparkHadoopUtil.get.conf)
      var stream: FSDataInputStream = null
      try {
        stream = fs.open(new Path(location.path))
        stream.seek(location.offset)
        Some(new BoundedInputStream(stream, location.length))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          if (stream != null) stream.close()
          None
      }
    }
  }
}