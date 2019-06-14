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

package org.archive.archivespark.sparkling.http

import java.io.{BufferedInputStream, InputStream}
import java.util.zip.GZIPInputStream

import org.apache.commons.httpclient.ChunkedInputStream
import org.apache.http.client.entity.DeflateInputStream
import org.archive.archivespark.sparkling.io.IOUtil
import org.archive.archivespark.sparkling.util.StringUtil

import scala.collection.immutable.ListMap
import scala.util.Try

class HttpMessage (val statusLine: String, val headers: Map[String, String], val payload: InputStream) {
  import HttpMessage._

  lazy val lowerCaseHeaders: Map[String, String] = headers.map{case (k,v) => (k.toLowerCase, v)}

  def contentEncoding: Option[String] = lowerCaseHeaders.get("content-encoding").map(_.toLowerCase)
  def mime: Option[String] = lowerCaseHeaders.get("content-type").map(_.split(';').head.trim.toLowerCase)
  def charset: Option[String] = {
    lowerCaseHeaders.get("content-type").flatMap(_.split(';').drop(1).headOption).map(_.trim)
      .filter(_.startsWith("charset="))
      .map(_.drop(8).trim.stripPrefix("\"").stripPrefix("'").stripSuffix("'").stripSuffix("\"").split(",", 2).head.trim)
      .filter(_.nonEmpty).map(_.toUpperCase)
  }
  def redirectLocation: Option[String] = lowerCaseHeaders.get("location").map(_.trim)
  def isChunked: Boolean = lowerCaseHeaders.get("transfer-encoding").map(_.toLowerCase).contains("chunked")

  def status: Int = statusLine.split(" +").drop(1).headOption.flatMap(s => Try{s.toInt}.toOption).getOrElse(-1)

  lazy val body: InputStream = Try {
    var decoded = if (isChunked) new ChunkedInputStream(payload) else payload
    val decoders = contentEncoding.toSeq.flatMap(_.split(',').map(_.trim).flatMap(DecoderRegistry.get))
    for (decoder <- decoders) decoded = decoder(decoded)
    new BufferedInputStream(decoded)
  }.getOrElse(IOUtil.emptyStream)

  lazy val bodyString: String = StringUtil.fromInputStream(body, charset.toSeq ++ BodyCharsets)
}

object HttpMessage {
  val Charset: String = "UTF-8"
  val HttpMessageStart = "HTTP/"
  val BodyCharsets: Seq[String] = Seq("UTF-8", "ISO-8859-1", "WINDOWS-1252")

  // see org.apache.http.client.protocol.ResponseContentEncoding
  val DecoderRegistry: Map[String, InputStream => InputStream] = Map(
    "gzip" -> ((in: InputStream) => new GZIPInputStream(in)),
    "x-gzip" -> ((in: InputStream) => new GZIPInputStream(in)),
    "deflate" -> ((in: InputStream) => new DeflateInputStream(in))
  )

  def get(in: InputStream): Option[HttpMessage] = {
    var line = StringUtil.readLine(in, Charset)
    while (line != null && !{
      if (line.startsWith(HttpMessageStart)) {
        val statusLine = line
        val headers = collection.mutable.Buffer.empty[(String, String)]
        line = StringUtil.readLine(in, Charset)
        while (line != null && line.trim.nonEmpty) {
          val split = line.split(":", 2)
          if (split.length == 2) headers += ((split(0).trim, split(1).trim))
          line = StringUtil.readLine(in, Charset)
        }
        return Some(new HttpMessage(statusLine, ListMap(headers: _*), in))
      }
      false
    }) line = StringUtil.readLine(in, Charset)
    None
  }
}
