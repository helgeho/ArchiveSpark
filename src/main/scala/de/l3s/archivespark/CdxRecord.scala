package de.l3s.archivespark

import com.github.nscala_time.time.Imports._

object CdxRecord {
  val pattern = s"^${(1 to 11).map(_ => "([^ ]+)").mkString("[ ]+")}$$".r // 11 fields separated by spaces

  def fromString(str: String): CdxRecord = {
    str match {
      case pattern(url, timeStr, fullUrl, mimeType, statusStr, checksum, redirectUrl, meta, sizeStr, offsetStr, filename) =>
        try {
          val time = DateTimeFormat.forPattern("yyyyMMddkkmmss").parseDateTime(timeStr)
          CdxRecord(url, time, fullUrl, mimeType, statusStr.toInt, checksum, redirectUrl, meta, new LocationInfo(sizeStr.toLong, offsetStr.toLong, filename))
        } catch {
          case e: Exception => null // skip, malformed
        }
      case _ => null // skip, malformed
    }
  }
}

/**
 * Created by holzmann on 04.08.2015.
 */
case class CdxRecord(surtUrl: String,
                     timestamp: DateTime,
                     originalUrl: String,
                     mime: String,
                     status: Int,
                     digest: String,
                     redirectUrl: String,
                     meta: String,
                     location: LocationInfo) {
  def toJson: Map[String, Any] = Map[String, Any](
    "surtUrl" -> surtUrl,
    "timestamp" -> timestamp.toString,
    "originalUrl" -> originalUrl,
    "mime" -> mime,
    "status" -> status.toString,
    "digest" -> digest,
    "redirectUrl" -> redirectUrl,
    "meta" -> meta
  )
}
