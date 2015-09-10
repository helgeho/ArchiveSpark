package de.l3s.archivespark

import de.l3s.archivespark.utils.Json._
import org.apache.spark.SparkContext

/**
 * Created by holzmann on 07.08.2015.
 */
abstract class ArchiveRecord(val get: CdxRecord)(implicit sc: SparkContext) {
  def resolve(cdx: ResolvedCdxRecord): ResolvedArchiveRecord

  override def toString: String = mapToJson(toJson)

  def toJson: Map[String, Any] = Map[String, Any]("record" -> this.get.toJson)
}
