package de.l3s.archivespark

/**
 * Created by holzmann on 05.08.2015.
 */
class ArchiveRecordField[T] private (val get: T) extends Enrichable[T, ArchiveRecordField, ArchiveRecordField] {
  override def toJson: String = ??? // TODO: to be implemented
}

object ArchiveRecordField {
  def apply[T](value: T): ArchiveRecordField[T] = new ArchiveRecordField[T](value)
}