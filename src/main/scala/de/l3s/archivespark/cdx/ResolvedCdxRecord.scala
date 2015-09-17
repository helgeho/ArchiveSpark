package de.l3s.archivespark.cdx

import de.l3s.archivespark.utils.JsonConvertible

/**
 * Created by holzmann on 07.08.2015.
 */
class ResolvedCdxRecord(original: CdxRecord, locationPath: String, val parentRecord: CdxRecord) extends JsonConvertible {
  def surtUrl = original.surtUrl
  def timestamp = original.timestamp
  def originalUrl = original.originalUrl
  def mime = original.mime
  def status = original.status
  def digest = original.digest
  def redirectUrl = original.redirectUrl
  def meta = original.meta
  val location = LocationInfo(original.location.compressedSize, original.location.offset, original.location.filename, locationPath)

  def toJson: Map[String, Any] = {
    if (parentRecord != null) {
      original.toJson + ("parent" -> parentRecord.toJson)
    } else {
      original.toJson
    }
  }
}
