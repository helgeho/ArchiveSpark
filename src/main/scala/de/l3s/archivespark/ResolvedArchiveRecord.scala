package de.l3s.archivespark

import java.io.InputStream

import de.l3s.archivespark.cdx.ResolvedCdxRecord
import de.l3s.archivespark.enrich.EnrichRoot
import de.l3s.archivespark.utils.Json._

/**
 * Created by holzmann on 04.08.2015.
 */
abstract class ResolvedArchiveRecord(override val get: ResolvedCdxRecord) extends EnrichRoot[ResolvedCdxRecord, ResolvedArchiveRecord] {
  def access[R >: Null](action: (String, InputStream) => R): R

  override def toJson: Map[String, Any] = (if (isExcludedFromOutput) Map[String, Any]() else Map(
    "record" -> json(this.get)
  )) ++ enrichments.map{ case (name, field) => (name, mapToAny(field.toJson)) }.filter{ case (_, field) => field != null }

  def copy(): ResolvedArchiveRecord = clone().asInstanceOf[ResolvedArchiveRecord]
}
