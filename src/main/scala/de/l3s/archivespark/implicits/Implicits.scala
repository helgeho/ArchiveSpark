package de.l3s.archivespark.implicits

import de.l3s.archivespark.cdx.{CdxRecord, ResolvedCdxRecord}
import de.l3s.archivespark.{ArchiveRecord, ResolvedArchiveRecord}

object Implicits extends Implicits

/**
 * Created by holzmann on 11.09.2015.
 */
class Implicits {
  implicit def resolvedArchiveRecordToResolvedCdxRecord(record: ResolvedArchiveRecord): ResolvedCdxRecord = record.get
  implicit def archiveRecordToCdxRecord(record: ArchiveRecord): CdxRecord = record.get
}
