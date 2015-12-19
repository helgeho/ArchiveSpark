/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.l3s.archivespark.cdx

import com.github.nscala_time.time.Imports._
import de.l3s.archivespark.utils.JsonConvertible

import scala.util.Try

object CdxRecord {
  val pattern = s"^${(1 to 11).map(_ => "([^ ]+)").mkString("[ ]+")}$$".r // 11 fields separated by spaces
  val dateTimeFormat = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  def fromString(str: String): CdxRecord = {
    str match {
      case pattern(url, timestamp, fullUrl, mimeType, statusStr, checksum, redirectUrl, meta, sizeStr, offsetStr, filename) =>
        try {
          val status = Try(statusStr.toInt).getOrElse(-1)
          CdxRecord(url, timestamp, fullUrl, mimeType, statusStr.toInt, checksum, redirectUrl, meta, new LocationInfo(sizeStr.toLong, offsetStr.toLong, filename))
        } catch {
          case e: Exception => null // skip, malformed
        }
      case _ => null // skip, malformed
    }
  }
}

case class CdxRecord(surtUrl: String,
                     timestamp: String,
                     originalUrl: String,
                     mime: String,
                     status: Int,
                     digest: String,
                     redirectUrl: String,
                     meta: String,
                     location: LocationInfo) extends JsonConvertible {
  def time = Try(CdxRecord.dateTimeFormat.parseDateTime(timestamp)).getOrElse(null)

  def toSimpleString = {
    val statusStr = if (status < 0) "-" else status.toString
    s"$surtUrl\t$timestamp\t$originalUrl\t$mime\t$statusStr\t$digest\t$redirectUrl\t$meta"
  }

  def toJson: Map[String, Any] = Map[String, Any](
    "surtUrl" -> surtUrl,
    "timestamp" -> timestamp,
    "originalUrl" -> originalUrl,
    "mime" -> mime,
    "status" -> status,
    "digest" -> digest,
    "redirectUrl" -> redirectUrl,
    "meta" -> meta
  )
}
