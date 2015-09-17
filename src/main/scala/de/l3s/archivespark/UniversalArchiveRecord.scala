package de.l3s.archivespark

/**
 * Created by holzmann on 07.08.2015.
 */
class UniversalArchiveRecord(cdx: CdxRecord) extends ArchiveRecord(cdx) {
  override def resolve(cdx: ResolvedCdxRecord): ResolvedArchiveRecord = {
    val location = cdx.location.fileLocation // resolved location
    val protocol = location.toLowerCase.split("\\:\\/\\/").head // prefix before ://, e.g. http in http://www...
    protocol match {
      case "hdfs" => new ResolvedHdfsArchiveRecord(cdx)
      case _ => null
    }
  }
}
