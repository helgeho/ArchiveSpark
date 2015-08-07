package de.l3s.archivespark

import org.apache.spark.SparkContext

/**
 * Created by holzmann on 04.08.2015.
 */
object ArchiveSpark {
  def hdfs(cdxPath: String, warcPath: String)(implicit sc: SparkContext): ArchiveRDD = HdfsArchiveRDD(cdxPath, warcPath)
}
