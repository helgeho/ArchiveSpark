/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2017 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.specific.warc.specs

import java.io.ByteArrayInputStream

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.dataspecs.DataSpec
import de.l3s.archivespark.dataspecs.access.RawBytesAccessor
import de.l3s.archivespark.specific.warc.{CdxRecord, RawArchiveRecord, WarcHeaders, WarcRecord}
import de.l3s.archivespark.utils.{GZipBytes, SURT}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.archive.io.warc.WARCReaderFactory

import scala.collection.JavaConverters._
import scala.util.Try

class WarcHdfsSpec private(paths: String) extends DataSpec[(String, Array[Byte]), WarcRecord] {
  def load(sc: SparkContext, minPartitions: Int): RDD[(String, Array[Byte])] = {
    sc.binaryFiles(paths, ArchiveSpark.partitions(sc)).flatMap{case (filename, stream) =>
      var dummyFilename = filename.toLowerCase.stripSuffix(".gz").stripSuffix(".arc")
      if (dummyFilename.endsWith(".warc")) dummyFilename += ".gz"
      else dummyFilename += ".warc.gz"

      var reader = WARCReaderFactory.get(filename, stream.open(), true)
      reader.setStrict(false)
      reader.iterator().asScala.flatMap{record =>
        Try {
          val header = record.getHeader
          val raw = new RawArchiveRecord(record)
          var payload = raw.payload

          val dummyHeaderStr = StringBuilder.newBuilder
          dummyHeaderStr.append("WARC/1.0").append(WarcHeaders.Br)
          header.getHeaderFields.asScala.foreach{case (key, value) => dummyHeaderStr.append(key + ": " + value).append(WarcHeaders.Br)}
          dummyHeaderStr.append("Content-Length: " + payload.length).append(WarcHeaders.Br).append(WarcHeaders.Br)

          val dummyHeaderBytes = dummyHeaderStr.toString.getBytes(WarcHeaders.UTF8)

          val bytes = dummyHeaderBytes ++ payload
          (dummyFilename, GZipBytes(bytes))
        }.toOption
      }
    }
  }

  override def parse(raw: (String, Array[Byte])): Option[WarcRecord] = Try {
    val (dummyFilename, bytes) = raw
    var record = de.l3s.concatgz.data.WarcRecord.get(dummyFilename, new ByteArrayInputStream(bytes))
    val header = record.getHeader
    val url = header.getUrl
    val redirect = record.getHttpHeaders.asScala.find{case (k,v) => v.toLowerCase == "location"}.map(_._2).getOrElse("-")
    val mime = Option(record.getHttpMimeType).getOrElse("-")
    val status = record.getHttpResponse.getMessage.getStatus
    val archiveRecord = record.getRecord
    archiveRecord.close()
    val digest = Option(archiveRecord.getDigestStr).getOrElse("-")
    var date = header.getDate.replaceAll("[^\\d]", "").take(14)
    val cdx = CdxRecord(SURT.fromUrl(url), date, url, mime, status, digest, redirect, "-", bytes.length.toLong)
    new WarcRecord(cdx, dummyFilename, new RawBytesAccessor(bytes))
  }.toOption
}

object WarcHdfsSpec {
  def apply(paths: String) = new WarcHdfsSpec(paths)
}