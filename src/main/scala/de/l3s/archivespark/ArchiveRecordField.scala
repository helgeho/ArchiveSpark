package de.l3s.archivespark

import de.l3s.archivespark.utils.Json._

/**
 * Created by holzmann on 05.08.2015.
 */
class ArchiveRecordField[T] private (val get: T) extends Enrichable[T, ArchiveRecordField[T]] {
  override def toJson: Map[String, Any] = Map[String, Any](
      null.asInstanceOf[String] -> this.get
    ) ++ enrichments.map{ case (name, field) => (name, mapToAny(field.toJson)) }

  override def copy(): Any = clone()
}

object ArchiveRecordField {
  def apply[T](value: T): ArchiveRecordField[T] = new ArchiveRecordField[T](value)
}