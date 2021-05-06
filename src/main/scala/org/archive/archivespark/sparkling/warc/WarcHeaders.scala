/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2018 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package org.archive.archivespark.sparkling.warc

import java.nio.charset.Charset
import java.util.UUID

import org.archive.archivespark.sparkling.Sparkling
import org.archive.archivespark.sparkling.util.DigestUtil
import org.joda.time.Instant
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

object WarcHeaders {
  val UTF8: Charset = Charset.forName(Sparkling.DefaultCharset)
  val ArcDateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC
  val WarcDateTimeFormat: DateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis

  val Br = "\r\n"

  def arcFile(info: WarcFileMeta, filename: String): Array[Byte] = {
    val header = StringBuilder.newBuilder
    header.append("filedesc://")
    header.append(filename)
    header.append(" 0.0.0.0 ")
    header.append(ArcDateTimeFormat.print(info.created))
    header.append(" text/plain ")

    val headerBody = StringBuilder.newBuilder
    // Internet Archive: Name of gathering organization with no white space (http://archive.org/web/researcher/ArcFileFormat.php)
    headerBody.append("1 0 " + info.publisher.replace(" ", "")).append(Br)
    headerBody.append("URL IP-address Archive-date Content-type Archive-length").append(Br)

    val headerBodyStr: String = headerBody.toString
    val headerBodyBlob: Array[Byte] = headerBodyStr.getBytes(UTF8)

    header.append(headerBodyBlob.length).append(Br)
    header.append(headerBodyStr).append(Br)

    header.toString().getBytes(UTF8)
  }

  def warcFile(meta: WarcFileMeta, filename: String): Array[Byte] = {
    val header = StringBuilder.newBuilder
    header.append("WARC/1.0").append(Br)
    header.append("WARC-Type: warcinfo").append(Br)
    header.append("WARC-Date: " + WarcDateTimeFormat.print(Instant.now)).append(Br)
    header.append("WARC-Filename: " + filename).append(Br)
    header.append("WARC-Record-ID: " + newRecordID()).append(Br)
    header.append("Content-Type: application/warc-fields").append(Br)

    val headerBody = StringBuilder.newBuilder
    headerBody.append("software: " + meta.software).append(Br)
    headerBody.append("format: WARC File Format 1.0").append(Br)
    headerBody.append("conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf").append(Br)
    headerBody.append("publisher: " + meta.publisher).append(Br)
    headerBody.append("created: " + WarcDateTimeFormat.print(meta.created)).append(Br)
    headerBody.append(Br * 3)

    val headerBodyStr = headerBody.toString()
    val headerBodyBlob = headerBodyStr.getBytes(UTF8)

    header.append("Content-Length: " + headerBodyBlob.length).append(Br)
    header.append(Br)
    header.append(headerBodyStr)

    header.toString().getBytes(UTF8)
  }

  def warcRecord(warcType: String, meta: WarcRecordMeta, contentLength: Long, payloadDigest: Option[String]): Array[Byte] = {
    val header = StringBuilder.newBuilder
    header.append("WARC/1.0").append(Br)
    header.append("WARC-Type: " + warcType).append(Br)
    header.append("WARC-Target-URI: " + meta.url).append(Br)
    header.append("WARC-Date: " + WarcDateTimeFormat.print(meta.timestamp)).append(Br)
    for (digest <- payloadDigest) header.append("WARC-Payload-Digest: " + digest).append(Br)
    for (ip <- meta.ip) header.append("WARC-IP-Address: " + ip).append(Br)
    header.append("WARC-Record-ID: " + meta.recordId.getOrElse(newRecordID())).append(Br)
    header.append("Content-Type: application/http; msgtype=" + warcType).append(Br)
    header.append("Content-Length: " + contentLength).append(Br)
    header.append(Br)

    header.toString().getBytes(UTF8)
  }

  def warcResponseRecord(meta: WarcRecordMeta, content: Array[Byte], payload: Array[Byte]): Array[Byte] = {
    warcRecord("response", meta, content.length, Some("sha1:" + DigestUtil.sha1Base32(payload)))
  }

  def http(statusLine: String, headers: Seq[(String, String)]): Array[Byte] = {
    val header = StringBuilder.newBuilder
    header.append(statusLine).append(Br)
    for ((key, value) <- headers) {
      header.append(s"$key: $value").append(Br)
    }
    header.append(Br)
    header.toString().getBytes(UTF8)
  }

  private def newRecordID(): String = "<urn:uuid:" + UUID.randomUUID() + ">"
}
