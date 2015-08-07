package de.l3s.archivespark

import com.github.nscala_time.time.Imports._

object CdxRecord {
  val pattern = s"^${(1 to 11).map(_ => "([^ ]+)").mkString("[ ]+")}$$".r // 11 fields separated by spaces

  def fromString(str: String): CdxRecord = {
    str match {
      case pattern(url, timeStr, fullUrl, mimeType, statusStr, checksum, redirectUrl, meta, sizeStr, offsetStr, filename) =>
        try {
          val time = DateTimeFormat.forPattern("yyyyMMddkkmmss").parseDateTime(timeStr)
          CdxRecord(url, time, fullUrl, mimeType, statusStr.toInt, checksum, redirectUrl, meta, sizeStr.toLong, offsetStr.toLong, filename)
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
case class CdxRecord(url: String,
                     time: DateTime,
                     fullUrl: String,
                     mimeType: String,
                     status: Int,
                     checksum: String,
                     redirectUrl: String,
                     meta: String,
                     size: Long,
                     offset: Long,
                     filename: String)
