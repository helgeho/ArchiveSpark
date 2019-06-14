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

package org.archive.archivespark.specific.warc.specs

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.archive.archivespark.dataspecs.DataSpec
import org.archive.archivespark.dataspecs.access.ByteArrayAccessor
import org.archive.archivespark.sparkling.io.ByteArray
import org.archive.archivespark.sparkling.util.{IteratorUtil, RddUtil}
import org.archive.archivespark.sparkling.warc.{WarcLoader, WarcRecord => WARC}
import org.archive.archivespark.specific.warc.WarcRecord

class WarcHdfsSpec private(path: String) extends DataSpec[(String, Long, ByteArray), WarcRecord] {
  def load(sc: SparkContext, minPartitions: Int): RDD[(String, Long, ByteArray)] = {
    RddUtil.loadBinary(path, decompress = false, close = false) { case (file, stream) =>
      IteratorUtil.cleanup(WarcLoader.loadBytes(stream).map { case (offset, bytes) =>
        (file, offset, bytes)
      }, stream.close)
    }
  }

  override def parse(in: (String, Long, ByteArray)): Option[WarcRecord] = {
    val (file, offset, bytes) = in
    WARC.get(bytes.toInputStream).flatMap { warc =>
      warc.toCdx(bytes.length).map { cdx =>
        new WarcRecord(cdx.copy(additionalFields = Seq(offset.toString, file)), new ByteArrayAccessor(bytes))
      }
    }
  }
}

object WarcHdfsSpec {
  def apply(path: String) = new WarcHdfsSpec(path)
}