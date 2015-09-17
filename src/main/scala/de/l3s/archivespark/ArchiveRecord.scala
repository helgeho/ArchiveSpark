package de.l3s.archivespark

import de.l3s.archivespark.cdx.{CdxRecord, ResolvedCdxRecord}
import de.l3s.archivespark.utils.JsonConvertible

/**
 * Created by holzmann on 07.08.2015.
 */
abstract class ArchiveRecord(val get: CdxRecord) extends JsonConvertible with Serializable {
  def resolve(cdx: ResolvedCdxRecord): ResolvedArchiveRecord

  def toJson: Map[String, Any] = Map[String, Any]("record" -> this.get.toJson)
}
