package de.l3s.archivespark

import de.l3s.archivespark.cdx.{CdxRecord, ResolvedCdxRecord}
import de.l3s.archivespark.rdd.{UniversalArchiveRDD, HdfsArchiveRDD}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by holzmann on 04.08.2015.
 */
object ArchiveSpark {
  def load(cdxPath: String)(implicit sc: SparkContext): UniversalArchiveRDD = UniversalArchiveRDD(cdxPath)

  def hdfs(cdxPath: String, warcPath: String)(implicit sc: SparkContext): HdfsArchiveRDD = HdfsArchiveRDD(cdxPath, warcPath)

  def textFileWithPath(path: String)(implicit sc: SparkContext): RDD[(String, String)] = {
    sc.wholeTextFiles(path).flatMap{case (filename, content) => content.split("\n").map(line => (filename, line))}
  }

  def cdx(path: String)(implicit sc: SparkContext): RDD[CdxRecord] = {
    sc.textFile(path).map(line => CdxRecord.fromString(line)).filter(cdx => cdx != null)
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
