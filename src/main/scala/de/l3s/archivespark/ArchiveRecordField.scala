package de.l3s.archivespark

/**
 * Created by holzmann on 05.08.2015.
 */
class ArchiveRecordField[T](val get: T) extends Enrichable[T, ArchiveRecordField, ArchiveRecordField] {
  override def toJson: String = ??? // TODO: to be implemented
}
