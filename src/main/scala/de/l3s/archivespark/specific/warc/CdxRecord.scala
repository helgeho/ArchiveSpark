/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.specific.warc

import java.time.LocalDateTime

import de.l3s.archivespark.utils.JsonConvertible

import scala.util.Try

object CdxRecord {
  val DateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  def fromString(str: String): Option[CdxRecord] = {
    val split = str.trim.split("[ \t]")
    if (split.length < 7 || split.length == 8) return None
    val (url, timestamp, fullUrl, mimeType, statusStr, checksum, redirectUrl, meta, conpressedSizeStr) = if (split.length == 7) {
      (split(0), split(1), split(2), split(3), split(4), split(5), "-", "-", split(6)) // CDX server
    } else {
      (split(0), split(1), split(2), split(3), split(4), split(5), split(6), split(7), split(8))
    }
    try {
      val status = Try(statusStr.toInt).getOrElse(-1)
      Some(CdxRecord(url, timestamp, fullUrl, mimeType, status, checksum, redirectUrl, meta, conpressedSizeStr.toLong, split.drop(9)))
    } catch {
      case e: Exception => None
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
                     compressedSize: Long,
                     additionalFields: Seq[String]) extends JsonConvertible {
  def time = Try(LocalDateTime.parse(timestamp, CdxRecord.DateTimeFormatter)).getOrElse(null)

  def toCdxString = {
    val statusStr = if (status < 0) "-" else status.toString
    s"$surtUrl $timestamp $originalUrl $mime $statusStr $digest $redirectUrl $meta $compressedSize"
  }

  def toJson: Map[String, Any] = Map[String, Any](
    "surtUrl" -> surtUrl,
    "timestamp" -> timestamp,
    "originalUrl" -> originalUrl,
    "mime" -> mime,
    "status" -> status,
    "digest" -> digest,
    "redirectUrl" -> redirectUrl,
    "meta" -> meta,
    "compressedSize" -> compressedSize
  )
}
