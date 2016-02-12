/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark

import de.l3s.archivespark.cdx.{BroadcastPathMapLocationInfo, CdxRecord, ResolvedCdxRecord}
import de.l3s.archivespark.rdd.{HdfsArchiveRDD, UniversalArchiveRDD}
import de.l3s.archivespark.records.{HdfsArchiveRecord, ResolvedHdfsArchiveRecord, ResolvedLocalArchiveRecord}
import de.l3s.archivespark.utils.{FilePathMap, HttpArchiveRecord, HttpResponse, IO}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ArchiveSpark {
  private var initialized = false

  var parallelism = 0

  def partitions(implicit sc: SparkContext) = if (parallelism > 0) parallelism else sc.defaultParallelism

  def initialize(sc: SparkContext): Unit = {
    if (initialized) return
    initialized = true
    initialize(sc.getConf)
  }

  def initialize(conf: SparkConf): Unit = {
    conf.registerKryoClasses(Array(
      classOf[ResolvedCdxRecord],
      classOf[ResolvedArchiveRecord],
      classOf[ArchiveRecord],
      classOf[HdfsArchiveRecord],
      classOf[UniversalArchiveRDD],
      classOf[ResolvedHdfsArchiveRecord],
      classOf[CdxRecord],
      classOf[ArchiveRecordField[_]],
      classOf[HttpResponse],
      classOf[HttpArchiveRecord]
    ))
  }

  def load(cdxPath: String)(implicit sc: SparkContext): UniversalArchiveRDD = UniversalArchiveRDD(cdxPath)

  def hdfs(cdxPath: String, warcPath: String)(implicit sc: SparkContext): HdfsArchiveRDD = HdfsArchiveRDD(cdxPath, warcPath)

  def files(cdxPath: String, warcPath: String): Iterable[ResolvedArchiveRecord] = {
    IO.lines(cdxPath).view
      .map(line => CdxRecord.fromString(line)).filter(cdx => cdx != null).map(cdx => new ResolvedCdxRecord(cdx, warcPath, null))
      .map(cdx => new ResolvedLocalArchiveRecord(cdx))
  }

  def cdx(path: String)(implicit sc: SparkContext): RDD[CdxRecord] = {
    initialize(sc)
    sc.textFile(path, partitions).map(line => CdxRecord.fromString(line)).filter(cdx => cdx != null)
  }

  def resolvedCdx(path: String, warcPath: String)(implicit sc: SparkContext): RDD[ResolvedCdxRecord] = {
    implicit val filePathMap = sc.broadcast(FilePathMap(warcPath, Seq("(?i).*\\.arc\\.gz", "(?i).*\\.warc\\.gz")))
    cdx(path).map(cdx => new ResolvedCdxRecord(cdx, new BroadcastPathMapLocationInfo(cdx.location.compressedSize, cdx.location.offset, cdx.location.filename), null))
  }
}
