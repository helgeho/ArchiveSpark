package de.l3s.archivespark

import de.l3s.archivespark.utils.Json._

/**
 * Created by holzmann on 05.08.2015.
 */
class ArchiveRecordField[T] private (val get: T) extends Enrichable[T, ArchiveRecordField, ArchiveRecordField] {
  override def toJson: Map[String, Any] = Map[String, Any](
      null -> this.get
    ) ++ enrichments.map{ case (name, field) => (name, mapToAny(field.toJson)) }
}

object ArchiveRecordField {
  def apply[T](value: T): ArchiveRecordField[T] = new ArchiveRecordField[T](value)
}