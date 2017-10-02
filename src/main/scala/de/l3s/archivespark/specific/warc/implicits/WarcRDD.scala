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

package de.l3s.archivespark.specific.warc.implicits

import java.io.PrintStream
import java.util.zip.GZIPOutputStream

import de.l3s.archivespark.enrich.EnrichFunc
import de.l3s.archivespark.enrich.dataloads.ByteContentLoad
import de.l3s.archivespark.enrich.functions.DataLoad
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.specific.warc.enrichfunctions.{HttpPayload, WarcPayload}
import de.l3s.archivespark.specific.warc.{WarcHeaders, WarcLikeRecord, WarcMeta, WarcRecordInfo}
import de.l3s.archivespark.utils.{GZipBytes, SparkIO}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object WarcRDD {
  val WarcIpField = "WARC-IP-Address"
}

class WarcRDD[WARC <: WarcLikeRecord : ClassTag](rdd: RDD[WARC]) {
  import WarcRDD._

  def saveAsWarc(path: String, info: WarcMeta, generateCdx: Boolean = true): Long = {
    val payloadEnrichFunc: EnrichFunc[WarcLikeRecord, _] = DataLoad(ByteContentLoad.Field)

    val gz = path.toLowerCase.endsWith(".gz")

    SparkIO.save(path, rdd.enrich(payloadEnrichFunc)) { (idx, records, open) =>
      val id = idx.toString.reverse.padTo(5, '0').reverse.mkString
      val warcSuffix = s"-$id.warc${if (gz) ".gz" else ""}"
      val cdxSuffix = s"-$id.cdx${if (gz) ".gz" else ""}"

      var processed = 0L
      if (records.nonEmpty) {
        val warcFilename = info.filename(warcSuffix)
        open(warcFilename) { warcStream =>
          val header = WarcHeaders.file(info, warcSuffix)
          val headerBytes = if (gz) GZipBytes(header) else header
          warcStream.write(headerBytes)

          var offset = headerBytes.length.toLong
          open(info.filename(cdxSuffix)) { cdxStream =>
            lazy val cdxCompressed = new GZIPOutputStream(cdxStream)
            lazy val cdxOut = if (gz) new PrintStream(cdxCompressed) else new PrintStream(cdxStream)

            processed = records.map { record =>
              val warcHeadersOpt = record.value[WarcLikeRecord, Map[String, String]](payloadEnrichFunc, WarcPayload.RecordHeaderField)
              val recordInfo = WarcRecordInfo(record.get.originalUrl, record.get.time, warcHeadersOpt.flatMap(_.get(WarcIpField)))
              val payload = record.value[WarcLikeRecord, Array[Byte]](payloadEnrichFunc, HttpPayload.PayloadField).get
              val recordHeader = WarcHeaders.responseRecord(info, recordInfo, payload)

              val httpStatusOpt = record.value[WarcLikeRecord, String](payloadEnrichFunc, HttpPayload.StatusLineField)
              val httpHeadersOpt = record.value[WarcLikeRecord, Map[String, String]](payloadEnrichFunc, HttpPayload.HeaderField)
              val httpHeader = if (httpStatusOpt.isDefined && httpHeadersOpt.isDefined) WarcHeaders.http(httpStatusOpt.get, httpHeadersOpt.get) else Array.empty[Byte]

              val recordBytes = if (gz) GZipBytes(recordHeader ++ httpHeader ++ payload) else recordHeader ++ httpHeader ++ payload
              warcStream.write(recordBytes)

              if (generateCdx) {
                val locationInfo = Array(offset.toString, warcFilename)
                cdxOut.println(record.get.copy(compressedSize = recordBytes.length).toCdxString(locationInfo))
              }

              offset += recordBytes.length

              1L
            }.sum

            if (generateCdx && gz) cdxCompressed.finish()
          }
        }
      }
      processed
    }
  }

  def toCdxStrings: RDD[String] = toCdxStrings()
  def toCdxStrings(includeAdditionalFields: Boolean = true): RDD[String] = rdd.map(_.toCdxString(includeAdditionalFields))

  def saveAsCdx(path: String): Unit = if (path.endsWith(".gz")) toCdxStrings.saveAsTextFile(path, classOf[GzipCodec]) else toCdxStrings.saveAsTextFile(path)
}