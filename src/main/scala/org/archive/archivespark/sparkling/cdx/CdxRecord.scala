/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.archivespark.sparkling.cdx

import org.archive.archivespark.sparkling.util.{RegexUtil, Time14Util}
import org.joda.time.DateTime

import scala.util.Try

object CdxRecord {
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
      Some(CdxRecord(url, RegexUtil.r("[^\\d]").replaceAllIn(timestamp, "").take(14), fullUrl, mimeType, status, checksum, redirectUrl, meta, conpressedSizeStr.toLong, split.drop(9)))
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
                     additionalFields: Seq[String] = Seq.empty) {
  def time: DateTime = Time14Util.parse(timestamp, fix = true)

  def toCdxString(additionalFields: Seq[String]): String = {
    val statusStr = if (status < 0) "-" else status.toString
    val additionalStr = if (additionalFields.nonEmpty) additionalFields.mkString(" ") else ""
    s"$surtUrl $timestamp $originalUrl $mime $statusStr $digest $redirectUrl $meta $compressedSize $additionalStr".trim
  }

  def toCdxString(includeAdditionalFields: Boolean = true): String = toCdxString(if (includeAdditionalFields) additionalFields else Seq.empty)

  def toCdxString: String = toCdxString()

  def locationFromAdditionalFields: (String, Long) = (additionalFields.drop(1).mkString(" "), additionalFields.head.toLong)
}