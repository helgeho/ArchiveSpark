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

package de.l3s.archivespark.specific.warc.implicits

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.specific.warc.CdxRecord
import org.apache.spark.rdd.RDD

object ResolvableRDD {
  val RevisitMime = "warc/revisit"
}

class ResolvableRDD(rdd: RDD[CdxRecord]) {
  import ResolvableRDD._

  def resolveRevisits(original: RDD[CdxRecord]): RDD[CdxRecord] = {
    val originalPaired = original.filter(r => r.mime != RevisitMime).map(r => (r.digest, r)).reduceByKey({ (r1, r2) =>
      if (r1.timestamp.compareTo(r2.timestamp) >= 0) r1 else r2
    }, ArchiveSpark.partitions)

    val responses = rdd.filter(r => r.mime != RevisitMime)
    val revisits = rdd.filter(r => r.mime == RevisitMime)

    revisits.map(r => (r.digest, r)).join(originalPaired, ArchiveSpark.partitions).map{case (digest, (revisit, record)) =>
      record.copy(timestamp = revisit.timestamp, surtUrl = revisit.surtUrl, originalUrl = revisit.originalUrl)
    }.union(responses)
  }

  def resolveRevisits(): RDD[CdxRecord] = resolveRevisits(rdd)

  def mapInfo[Info](map: CdxRecord => String, @transient infoMapping: RDD[(String, Info)]): RDD[(CdxRecord, Info)] = {
    val fileRecordPairs = rdd.map(r => (map(r), r))
    fileRecordPairs.join(infoMapping, ArchiveSpark.partitions).map{case (_, pair) => pair}
  }
}