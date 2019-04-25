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

import org.archive.archivespark.dataspecs.DataSpec
import org.archive.archivespark.dataspecs.access.RawBytesAccessor
import org.archive.archivespark.specific.warc.{CdxRecord, WarcRecord}
import org.archive.archivespark.utils.SURT
import de.l3s.concatgz.io.warc.{WarcGzInputFormat, WarcWritable}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

import scala.util.Try

class WarcGzHdfsSpec private(paths: String) extends DataSpec[(NullWritable, WarcWritable), WarcRecord] {
  def load(sc: SparkContext, minPartitions: Int): RDD[(NullWritable, WarcWritable)] = {
    sc.newAPIHadoopFile(paths, classOf[WarcGzInputFormat], classOf[NullWritable], classOf[WarcWritable])
  }

  override def parse(raw: (NullWritable, WarcWritable)): Option[WarcRecord] = {
    Try {
      val (_, warc) = raw
      val record = warc.getRecord
      val header = record.getHeader
      val bytes = warc.getBytes.read
      val url = header.getUrl
      val redirect = record.getHttpHeaders.asScala.find{case (k,v) => v.toLowerCase == "location"}.map(_._2).getOrElse("-")
      val mime = Option(record.getHttpMimeType).getOrElse("-")
      val status = record.getHttpResponse.getMessage.getStatus
      val archiveRecord = record.getRecord
      archiveRecord.close()
      val digest = Option(archiveRecord.getDigestStr).getOrElse("-")
      var date = header.getDate.replaceAll("[^\\d]", "").take(14)
      val cdx = CdxRecord(SURT.fromUrl(url), date, url, mime, status, digest, redirect, "-", bytes.length.toLong, Seq(warc.getOffset.toString, warc.getFilename))
      new WarcRecord(cdx, warc.getFilename, new RawBytesAccessor(bytes))
    }.toOption
  }
}

object WarcGzHdfsSpec {
  def apply(paths: String) = new WarcGzHdfsSpec(paths)
}