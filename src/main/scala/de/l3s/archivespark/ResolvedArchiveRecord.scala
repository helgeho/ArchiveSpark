package de.l3s.archivespark

import java.io.InputStream

import de.l3s.archivespark.utils.Json._

/**
 * Created by holzmann on 04.08.2015.
 */
abstract class ResolvedArchiveRecord(override val get: ResolvedCdxRecord) extends EnrichRoot[ResolvedCdxRecord, ResolvedArchiveRecord, ArchiveRecordField] {
  def access[R >: Null](action: (String, InputStream) => R): R

  override def toString: String = mapToJson(toJson)

  override def toJson: Map[String, Any] = Map[String, Any](
    "record" -> this.get.toJson
  ) ++ enrichments.map{ case (name, field) => (name, mapToAny(field.toJson)) }
}
