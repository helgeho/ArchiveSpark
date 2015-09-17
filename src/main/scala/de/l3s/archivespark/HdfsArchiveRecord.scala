package de.l3s.archivespark

/**
 * Created by holzmann on 07.08.2015.
 */
class HdfsArchiveRecord(cdx: CdxRecord) extends ArchiveRecord(cdx) {
  override def resolve(resolved: ResolvedCdxRecord): ResolvedArchiveRecord = new ResolvedHdfsArchiveRecord(resolved)
}
