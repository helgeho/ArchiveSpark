package de.l3s.archivespark

import org.apache.spark.SparkContext

/**
 * Created by holzmann on 07.08.2015.
 */
class HdfsArchiveRecord(rdd: HdfsArchiveRDD, cdx: CdxRecord)(implicit val sc: SparkContext) extends ArchiveRecord(cdx) {
  override def resolve(cdx: ResolvedCdxRecord): ResolvedArchiveRecord = new ResolvedHdfsArchiveRecord(rdd, cdx)
}
