package de.l3s.archivespark

import org.apache.spark.SparkContext

/**
 * Created by holzmann on 07.08.2015.
 */
abstract class ArchiveRecord(val cdx: CdxRecord)(implicit sc: SparkContext) {
  def resolve(cdx: ResolvedCdxRecord): ResolvedArchiveRecord
}
