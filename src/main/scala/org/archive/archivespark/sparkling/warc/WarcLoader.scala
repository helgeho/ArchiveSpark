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

package org.archive.archivespark.sparkling.warc

import java.io._

import com.google.common.io.CountingInputStream
import org.apache.spark.rdd.RDD
import org.archive.archivespark.sparkling.io.{ByteArray, GzipUtil, MemoryBufferInputStream}
import org.archive.archivespark.sparkling.util._

import scala.util.Try

object WarcLoader {
  def loadBytes(in: InputStream): Iterator[(Long, ByteArray)] = {
    var pos = 0L
    val buffered = new MemoryBufferInputStream(in)
    val counting = new CountingInputStream(buffered)
    val records = load(counting)
    records.map { record =>
      record.close()
      val offset = pos
      pos = counting.getCount
      (offset, buffered.resetBuffer())
    }
  }

  def load(in: InputStream): Iterator[WarcRecord] = {
    var current: Option[WarcRecord] = None
    if (GzipUtil.isCompressed(in)) {
      GzipUtil.decompressConcatenated(in).flatMap { s =>
        IteratorUtil.whileDefined {
          if (current.isDefined) current.get.close()
          current = Try(WarcRecord.next(s)).getOrElse(None)
          current
        }
      }
    } else {
      IteratorUtil.whileDefined {
        if (current.isDefined) current.get.close()
        current = WarcRecord.next(in)
        current
      }
    }
  }

  def load(hdfsPath: String): RDD[WarcRecord] = RddUtil.loadBinary(hdfsPath, decompress = false, close = false) { (_, in) =>
    IteratorUtil.cleanup(load(in), in.close)
  }
}
