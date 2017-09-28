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

import de.l3s.archivespark.enrich.EnrichFunc
import de.l3s.archivespark.enrich.dataloads.ByteContentLoad
import de.l3s.archivespark.enrich.functions.DataLoad
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.specific.warc.enrichfunctions.HttpPayload
import de.l3s.archivespark.specific.warc.{WarcHeaders, WarcLikeRecord, WarcMeta, WarcRecordInfo}
import de.l3s.archivespark.utils.{GZipBytes, SparkIO}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class WarcRDD[WARC <: WarcLikeRecord : ClassTag](rdd: RDD[WARC]) {
  def saveAsWarc(path: String, info: WarcMeta): Long = {
    val payloadEnrichFunc: EnrichFunc[WarcLikeRecord, _] = DataLoad(ByteContentLoad.Field)

    val gz = path.toLowerCase.endsWith(".gz")

    SparkIO.save(path, rdd.enrich(payloadEnrichFunc)) { (idx, warcs, open) =>
      val fileSuffix = s"-$idx.warc${if (gz) ".gz" else ""}"

      open(info.filename(fileSuffix)) { stream =>
        val header = WarcHeaders.file(info, fileSuffix)
        stream.write(if (gz) GZipBytes(header) else header)

        for (warc <- warcs) {
          val httpHeadersOpt = warc.value[WarcLikeRecord, Map[String, String]](payloadEnrichFunc, HttpPayload.HeaderField)
          val payload = warc.value[WarcLikeRecord, Array[Byte]](payloadEnrichFunc, HttpPayload.PayloadField).get
          val recordInfo = WarcRecordInfo(warc.get.originalUrl, warc.get.time, httpHeadersOpt.flatMap(_.get("ip???????")))
          val recordHeader = WarcHeaders.responseRecord(info, recordInfo, payload)

          if (gz) stream.write(GZipBytes.open {gzip =>
              gzip.write(recordHeader)
              gzip.write(payload)
          }) else {
            stream.write(recordHeader)
            stream.write(payload)
          }
        }
      }
    }
  }
}