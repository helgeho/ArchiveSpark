package de.l3s.archivespark

/**
 * Created by holzmann on 11.09.2015.
 */
object Implicits {
  implicit def resolvedArchiveRecordToResolvedCdxRecord(record: ResolvedArchiveRecord): ResolvedCdxRecord = record.get
  implicit def archiveRecordToCdxRecord(record: ArchiveRecord): CdxRecord = record.get
}
