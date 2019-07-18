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

package org.archive.archivespark.specific.warc.implicits

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, PrintWriter}

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.archive.archivespark.model.dataloads.ByteLoad
import org.archive.archivespark.sparkling.io.{GzipBytes, HdfsIO, IOUtil}
import org.archive.archivespark.sparkling.util.{Common, RddUtil, StringUtil}
import org.archive.archivespark.sparkling.warc.{WarcHeaders, WarcRecord, WarcRecordMeta}
import org.archive.archivespark.specific.warc.functions.{HttpPayload, WarcPayloadFields}
import org.archive.archivespark.specific.warc.{WarcFileMeta, WarcLikeRecord}

import scala.reflect.ClassTag

object WarcRDD {
  val WarcIpField = "WARC-IP-Address"
  val WarcRecordIdField = "WARC-Record-ID"
}

class WarcRDD[WARC <: WarcLikeRecord : ClassTag](rdd: RDD[WARC]) {
  import WarcRDD._
  import org.archive.archivespark.sparkling.Sparkling._

  def saveAsWarc(path: String, meta: org.archive.archivespark.sparkling.warc.WarcFileMeta = WarcFileMeta(), generateCdx: Boolean = true): Long = {
    HdfsIO.ensureOutDir(path)

    val gz = path.toLowerCase.trim.endsWith(GzipExt)
    val filePrefix = StringUtil.stripSuffixes(new Path(path).getName, GzipExt, WarcExt, ArcExt)

    val emptyLines = {
      val bytes = new ByteArrayOutputStream()
      val print = new PrintWriter(bytes)
      print.println()
      print.println()
      print.flush()
      bytes.toByteArray
    }

    rdd.mapPartitionsWithIndex { case (idx, records) =>
      val warcPrefix = filePrefix + "-" + StringUtil.padNum(idx, 5)
      val warcFile = warcPrefix + WarcExt + (if (gz) GzipExt else "")
      val warcPath = new Path(path, warcFile).toString
      val warcCdxPath = new Path(path, warcPrefix + WarcExt + CdxExt + (if (gz) GzipExt else "")).toString

      var warcPosition = 0L
      val warcOut = Common.lazyValWithCleanup({
        val out = HdfsIO.out(warcPath, compress = false)
        val headerBytes = WarcHeaders.warcFile(meta, warcFile)
        val compressedHeader = if (gz) GzipBytes(headerBytes) else headerBytes
        out.write(compressedHeader)
        warcPosition += compressedHeader.length
        out
      })(_.close)

      val cdxOut = Common.lazyValWithCleanup(IOUtil.print(HdfsIO.out(warcCdxPath)))(_.close)

      val processed = records.map { record =>
        val payloadPointer = record.dataLoad(ByteLoad).get
        val enriched = payloadPointer.init(record, excludeFromOutput = false)

        val warcHeadersOpt = payloadPointer.sibling[Map[String, String]](WarcPayloadFields.RecordHeader).get(enriched)
        val recordMeta = WarcRecordMeta(enriched.get.originalUrl, enriched.get.time.toInstant, warcHeadersOpt.flatMap(_.get(WarcRecordIdField)), warcHeadersOpt.flatMap(_.get(WarcIpField)))

        val httpStatusOpt = payloadPointer.sibling[String](HttpPayload.StatusLineField).get(enriched)
        val httpHeadersOpt = payloadPointer.sibling[Map[String, String]](HttpPayload.HeaderField).get(enriched)
        val httpHeader = if (httpStatusOpt.isDefined && httpHeadersOpt.isDefined) WarcHeaders.http(httpStatusOpt.get, httpHeadersOpt.get) else Array.empty[Byte]

        payloadPointer.get(enriched) match {
          case Some(payload) =>
            val content = httpHeader ++ payload
            val recordHeader = WarcHeaders.warcResponseRecord(recordMeta, content, payload)

            val recordBytes = if (gz) GzipBytes(recordHeader ++ content ++ emptyLines) else recordHeader ++ content ++ emptyLines

            if (generateCdx) {
              val warc = WarcRecord.get(new ByteArrayInputStream(recordBytes))
              if (warc.isDefined) {
                val cdx = warc.get.toCdx(recordBytes.length)
                if (cdx.isDefined) {
                  warcOut.get.write(recordBytes)
                  val locationInfo = Array(warcPosition.toString, warcFile)
                  cdxOut.get.println(cdx.get.toCdxString(locationInfo))
                  warcPosition += recordBytes.length
                  1L
                } else 0L
              } else 0L
            } else {
              warcOut.get.write(recordBytes)
              warcPosition += recordBytes.length
              1L
            }
          case None => 0L
        }
      }.sum

      warcOut.clear(true)
      cdxOut.clear(true)

      Iterator(processed)
    }
  }.reduce(_ + _)

  def toCdxStrings: RDD[String] = toCdxStrings()
  def toCdxStrings(includeAdditionalFields: Boolean = true): RDD[String] = rdd.map(_.toCdxString(includeAdditionalFields))

  def saveAsCdx(path: String): Unit = RddUtil.saveAsTextFile(toCdxStrings, path)
}