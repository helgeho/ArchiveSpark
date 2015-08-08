package de.l3s.archivespark

import java.io.InputStream

/**
 * Created by holzmann on 04.08.2015.
 */
abstract class ResolvedArchiveRecord(val get: ResolvedCdxRecord) extends EnrichRoot[ResolvedCdxRecord, ResolvedArchiveRecord, ArchiveRecordField] {
  def access[R >: Null](action: (String, InputStream) => R): R

  override def toJson: String = ??? // TODO: to be implemented
}
