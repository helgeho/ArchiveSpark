package org.archive.archivespark.sparkling.http

import java.io.{BufferedInputStream, InputStream}
import java.util.zip.{DeflaterInputStream, GZIPInputStream}

import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream
import org.apache.commons.httpclient.ChunkedInputStream
import org.apache.commons.io.input.BoundedInputStream
import org.archive.archivespark.sparkling.io.IOUtil
import org.archive.archivespark.sparkling.util.StringUtil

import scala.util.Try

class HttpMessage(val statusLine: String, val headers: Seq[(String, String)], val payload: InputStream, maxBodyLength: Long = -1) {
  import HttpMessage._

  lazy val headerMap: Map[String, String] = headers.map { case (k, v) => (k.toLowerCase, v) }.toMap

  def contentEncoding: Option[String] = headerMap.get("content-encoding").map(_.toLowerCase)
  def contentLength: Option[Long] = headerMap.get("content-length").flatMap(l => Try { l.trim.toLong }.toOption)
  def mime: Option[String] = headerMap.get("content-type").map(StringUtil.prefixBySeparator(_, ";").trim.toLowerCase)
  def charset: Option[String] = {
    headerMap.get("content-type").flatMap(_.split(';').drop(1).headOption).map(_.trim).filter(_.startsWith("charset="))
      .map(_.drop(8).trim.stripPrefix("\"").stripPrefix("'").stripSuffix("'").stripSuffix("\"").split(",", 2).head.trim).filter(_.nonEmpty).map(_.toUpperCase)
  }
  def redirectLocation: Option[String] = headerMap.get("location").map(_.trim)
  def isChunked: Boolean = headerMap.get("transfer-encoding").map(_.toLowerCase).contains("chunked")

  def status: Int = statusLine.split(" +").drop(1).headOption.flatMap(s => Try { s.toInt }.toOption).getOrElse(-1)

  lazy val body: InputStream = Try {
    val decoders = contentEncoding.toSeq.flatMap(_.split(',').map(_.trim).flatMap(DecoderRegistry.get))
    var decoded = if (isChunked) new ChunkedInputStream(payload) else payload
    for (decoder <- decoders) decoded = decoder(decoded)
    if (maxBodyLength < 0) new BufferedInputStream(decoded) else new BoundedInputStream(new BufferedInputStream(decoded), maxBodyLength)
  }.getOrElse(IOUtil.EmptyStream)

  lazy val bodyString: String = StringUtil.fromInputStream(body, charset.toSeq ++ BodyCharsets)

  def copy(statusLine: String = statusLine, headers: Seq[(String, String)] = headers, payload: InputStream = payload, maxBodyLength: Long = maxBodyLength): HttpMessage = {
    new HttpMessage(statusLine, headers, payload, maxBodyLength)
  }
}

object HttpMessage {
  val Charset: String = "UTF-8"
  val HttpMessageStart = "HTTP/"
  val BodyCharsets: Seq[String] = Seq("UTF-8", "ISO-8859-1", "WINDOWS-1252")
  val ResetBuffer = 1024

  // see org.apache.http.client.protocol.ResponseContentEncoding
  val DecoderRegistry: Map[String, InputStream => InputStream] = Map(
    "gzip" -> ((in: InputStream) => new GZIPInputStream(in)),
    "x-gzip" -> ((in: InputStream) => new GZIPInputStream(in)),
    "deflate" -> ((in: InputStream) => new DeflaterInputStream(in)),
    "br" -> ((in: InputStream) => new BrotliCompressorInputStream(in))
  )

  def get(in: InputStream): Option[HttpMessage] = {
    if (in.markSupported()) in.mark(ResetBuffer)
    var line = StringUtil.readLine(in, Charset)
    while (line != null && line.trim.isEmpty) line = StringUtil.readLine(in, Charset)
    if (line != null && line.startsWith(HttpMessageStart) || Try(line.split(' ').last).toOption.exists(_.startsWith(HttpMessageStart))) {
      val statusLine = line
      val headers = collection.mutable.Buffer.empty[(String, String)]
      line = StringUtil.readLine(in, Charset)
      while (line != null && line.trim.nonEmpty) {
        val split = line.split(":", 2)
        if (split.length == 2) headers += ((split(0).trim, split(1).trim))
        line = StringUtil.readLine(in, Charset)
      }
      Some(new HttpMessage(statusLine, headers, in))
    } else {
      if (in.markSupported()) Try(in.reset())
      None
    }
  }
}
