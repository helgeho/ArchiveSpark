package de.l3s.archivespark.records

import de.l3s.archivespark._
import de.l3s.archivespark.cdx.{CdxRecord, ResolvedCdxRecord}

/**
 * Created by holzmann on 07.08.2015.
 */
class HdfsArchiveRecord(cdx: CdxRecord) extends ArchiveRecord(cdx) {
  override def resolve(resolved: ResolvedCdxRecord): ResolvedArchiveRecord = new ResolvedHdfsArchiveRecord(resolved)
}
