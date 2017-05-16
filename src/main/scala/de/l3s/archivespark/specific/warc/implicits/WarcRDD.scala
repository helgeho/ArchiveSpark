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

import de.l3s.archivespark.enrich.dataloads.ByteContentLoad
import de.l3s.archivespark.enrich.functions.DataLoad
import de.l3s.archivespark.enrich.{EnrichFunc, EnrichRoot}
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.specific.warc.enrichfunctions.HttpPayload
import de.l3s.archivespark.specific.warc.{WarcHeaders, WarcLikeRecord, WarcMeta, WarcRecordInfo}
import org.apache.spark.rdd.RDD

class WarcRDD[WARC <: WarcLikeRecord](rdd: RDD[WARC]) {
  def saveAsWarc(info: WarcMeta): Long = {
    val payloadEnrichFunc: EnrichFunc[WarcLikeRecord, _] = DataLoad(ByteContentLoad.Field)
    rdd.enrich(payloadEnrichFunc).mapPartitionsWithIndex{case (idx, warcs) =>
      val fileSuffix = s"-$idx.warc.gz"
      val header = WarcHeaders.file(info, fileSuffix)

      for (warc <- warcs) {
        val httpHeadersOpt: Option[Map[String, String]] = warc.value(payloadEnrichFunc, HttpPayload.HeaderField)
        val payload: Array[Byte] = warc.value(payloadEnrichFunc, HttpPayload.PayloadField).get
        val recordInfo = WarcRecordInfo(warc.get.originalUrl, warc.get.time, httpHeadersOpt.flatMap(_.get("")))
        val recordHeader
      }

      Iterator(1L)
    }.reduce(_ + _)
  }
}