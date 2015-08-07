package de.l3s.archivespark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HdfsArchiveRDD {
  def apply(cdxPath: String, warcPath: String)(implicit sc: SparkContext): HdfsArchiveRDD = {
    new HdfsArchiveRDD(warcPath, sc.textFile(cdxPath).map(str => CdxRecord.fromString(str)))
  }
}

/**
 * Created by holzmann on 04.08.2015.
 */
class HdfsArchiveRDD private (val warcPath: String, prev: RDD[CdxRecord])(implicit sc: SparkContext) extends ArchiveRDD(prev) {
  override protected def record(record: CdxRecord): ArchiveRecord = new HdfsArchiveRecord(this, record)
}
