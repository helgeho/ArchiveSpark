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

package org.archive.archivespark.sparkling.warc

import java.io.InputStream

import org.apache.commons.io.input.BoundedInputStream
import org.archive.archivespark.sparkling.cdx.CdxRecord
import org.archive.archivespark.sparkling.http.HttpMessage
import org.archive.archivespark.sparkling.io.{GzipUtil, IOUtil}
import org.archive.archivespark.sparkling.logging.LogContext
import org.archive.archivespark.sparkling.util.{DigestUtil, RegexUtil, StringUtil, SurtUtil}

import scala.collection.immutable.ListMap
import scala.util.Try

class WarcRecord (val versionStr: String, val headers: Map[String, String], stream: InputStream) {
  import WarcRecord._

  lazy val lowerCaseHeaders: Map[String, String] = headers.map{case (k,v) => (k.toLowerCase, v)}

  def contentLength: Option[Long] = lowerCaseHeaders.get("content-length").flatMap(l => Try{l.trim.toLong}.toOption)
  def url: Option[String] = lowerCaseHeaders.get("warc-target-uri").map(_.trim)
  def contentType: Option[String] = lowerCaseHeaders.get("content-type").map(_.split(';').head.trim)
  def timestamp: Option[String] = lowerCaseHeaders.get("warc-date").map(RegexUtil.r("[^\\d]").replaceAllIn(_, "").take(14))
  def warcType: Option[String] = lowerCaseHeaders.get("warc-type").map(_.trim.toLowerCase)
  def isRevisit: Boolean = warcType.contains("revisit")
  def isResponse: Boolean = warcType.contains("response")

  lazy val payload: InputStream = contentLength match {
    case Some(length) =>
      val bounded = new BoundedInputStream(stream, length)
      bounded.setPropagateClose(false)
      bounded
    case None => stream
  }

  def close(): Unit = if (contentLength.isDefined) IOUtil.readToEnd(payload)

  lazy val isHttp: Boolean = contentType.contains("application/http")
  lazy val http: Option[HttpMessage] = HttpMessage.get(payload)

  def payloadDigest(hash: InputStream => String = defaultDigestHash): Option[String] = {
    val bytes = if (isHttp) http.map(_.payload) else Some(payload)
    bytes.map(hash)
  }

  def toCdx(compressedSize: Long, digest: InputStream => String = defaultDigestHash, handleRevisits: Boolean = true, handleOthers: Boolean = false): Option[CdxRecord] = {
    if (isResponse || (handleRevisits && isRevisit) || handleOthers) {
      val surt = SurtUtil.fromUrl(url.get)
      val mime = if (isResponse) if (isHttp) http.flatMap(_.mime).getOrElse("-") else "-" else "warc/" + warcType
      val status = if (isHttp) http.map(_.status).getOrElse(-1) else -1
      val redirectUrl = if (isHttp) http.flatMap(_.redirectLocation).getOrElse("-") else "-"
      Some(CdxRecord(surt, timestamp.get, url.get, mime, status, payloadDigest(digest).getOrElse("-"), redirectUrl, "-", compressedSize))
    } else None
  }
}

object WarcRecord {
  implicit val logContext: LogContext = LogContext(this)

  val Charset = "UTF-8"
  val WarcRecordStart = "WARC/"

  def defaultDigestHash(in: InputStream): String = "sha1:" + DigestUtil.sha1Base32(in)

  def get(in: InputStream, handleArc: Boolean = true, autodetectCompressed: Boolean = true, compressed: Boolean = false): Option[WarcRecord] = {
    next(if ((autodetectCompressed && GzipUtil.isCompressed(in)) || (!autodetectCompressed && compressed)) GzipUtil.decompress(in) else in, handleArc)
  }

  def next(in: InputStream, handleArc: Boolean = true): Option[WarcRecord] = {
    var line = StringUtil.readLine(in, Charset)
    while (line != null && !{
      if (line.startsWith(WarcRecordStart)) {
        val versionStr = line
        val headers = collection.mutable.Buffer.empty[(String, String)]
        line = StringUtil.readLine(in, Charset)
        while (line != null && line.trim.nonEmpty) {
          val split = line.split(":", 2)
          if (split.length == 2) headers += ((split(0).trim, split(1).trim))
          line = StringUtil.readLine(in, Charset)
        }
        return Some(new WarcRecord(versionStr, ListMap(headers: _*), in))
      }
      false
    } && handleArc && !{
      if (RegexUtil.matchesAbsoluteUrlStart(line)) {
        val split = line.split(" ")
        // https://archive.org/web/researcher/ArcFileFormat.php
        if (split.length == 5) {
          val versionStr = "ARC/1"
          /*
URL-record-v1 == <url><sp>
<ip-address><sp>
<archive-date><sp>
<content-type><sp>
<length><nl>
          */
          val headers = ListMap(
            "WARC-Type" -> "response",
            "WARC-Target-URI" -> split(0),
            "WARC-Date" -> split(2),
            "WARC-IP-Address" -> split(1),
            "Content-Type" -> split(3),
            "Content-Length" -> split(4)
          )
          return Some(new WarcRecord(versionStr, headers, in))
        } else if (split.length == 10) {
          val versionStr = "ARC/2"
          /*
URL-record-v2 == <url><sp>
<ip-address><sp>
<archive-date><sp>
<content-type><sp>
<result-code><sp>
<checksum><sp>
<location><sp>
<offset><sp>
<filename><sp>
<length><nl>
          */
          val headers = ListMap(
            "WARC-Type" -> "response",
            "WARC-Target-URI" -> split(0),
            "WARC-Date" -> split(2),
            "WARC-IP-Address" -> split(1),
            "WARC-Payload-Digest" -> split(5),
            "ARC-Record-Location" -> (split(8) + ":" + split(7)),
            "Location" -> split(6),
            "Content-Type" -> split(3),
            "Result-Code" -> split(4),
            "Content-Length" -> split(9)
          )
          return Some(new WarcRecord(versionStr, headers, in))
        }
      }
      false
    }) line = StringUtil.readLine(in, Charset)
    None
  }
}
