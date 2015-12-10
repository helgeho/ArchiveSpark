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

import de.l3s.archivespark.cdx.{CdxRecord, ResolvedCdxRecord}
import de.l3s.archivespark.rdd.{UniversalArchiveRDD, HdfsArchiveRDD}
import de.l3s.archivespark.records.{ResolvedHdfsArchiveRecord, HdfsArchiveRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ArchiveSpark {
  private var initialized = false

  var parallelism = 0

  def partitions(implicit sc: SparkContext) = if (parallelism > 0) parallelism else sc.defaultParallelism

  def initialize(sc: SparkContext): Unit = {
    if (initialized) return
    initialized = true
    sc.getConf.registerKryoClasses(Array(
      classOf[ResolvedCdxRecord],
      classOf[ResolvedArchiveRecord],
      classOf[ArchiveRecord],
      classOf[HdfsArchiveRecord],
      classOf[UniversalArchiveRDD],
      classOf[ResolvedHdfsArchiveRecord],
      classOf[CdxRecord],
      classOf[ArchiveRecordField[_]]
    ))
  }

  def load(cdxPath: String)(implicit sc: SparkContext): UniversalArchiveRDD = UniversalArchiveRDD(cdxPath)

  def hdfs(cdxPath: String, warcPath: String)(implicit sc: SparkContext): HdfsArchiveRDD = HdfsArchiveRDD(cdxPath, warcPath)

  def textFileWithPath(path: String)(implicit sc: SparkContext): RDD[(String, String)] = {
    initialize(sc)
    sc.wholeTextFiles(path, partitions).flatMap{case (filename, content) => content.split("\n").map(line => (filename, line))}
  }

  def cdx(path: String)(implicit sc: SparkContext): RDD[CdxRecord] = {
    initialize(sc)
    sc.textFile(path, partitions).map(line => CdxRecord.fromString(line)).filter(cdx => cdx != null)
  }

  def resolvedCdx(path: String, warcPath: String)(implicit sc: SparkContext): RDD[ResolvedCdxRecord] = {
    cdx(path).map(cdx => new ResolvedCdxRecord(cdx, warcPath, null))
  }

  def cdxWithPath(path: String)(implicit sc: SparkContext): RDD[(String, CdxRecord)] = {
    textFileWithPath(path).map{case (p, line) => (p, CdxRecord.fromString(line))}.filter{case (_, cdx) => cdx != null}
  }

  def resolvedCdxWithPath(path: String, warcPath: String)(implicit sc: SparkContext): RDD[(String, ResolvedCdxRecord)] = {
    cdxWithPath(path).map{case (cdxPath, cdx) => (cdxPath, new ResolvedCdxRecord(cdx, warcPath, null))}
  }
}
