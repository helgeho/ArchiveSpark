package de.l3s.archivespark.rdd

import de.l3s.archivespark.cdx.ResolvedCdxRecord
import de.l3s.archivespark.records.ResolvedHdfsArchiveRecord
import de.l3s.archivespark.ArchiveSpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HdfsArchiveRDD {
  def apply(cdxPath: String, warcPath: String)(implicit sc: SparkContext): HdfsArchiveRDD = {
    new HdfsArchiveRDD(warcPath, ArchiveSpark.resolvedCdxWithPath(cdxPath, warcPath))
  }
}

class HdfsArchiveRDD private (val warcPath: String, parent: RDD[(String, ResolvedCdxRecord)]) extends ResolvedArchiveRDD[(String, ResolvedCdxRecord)](parent) {
  override protected def record(cdxWithPath: (String, ResolvedCdxRecord)) = {
    val (path, cdx) = cdxWithPath
    new ResolvedHdfsArchiveRecord(cdx, path)
  }
}