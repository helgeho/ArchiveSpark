/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2017 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.http

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI

import org.apache.commons.io.IOUtils
import org.archive.format.http.HttpResponseParser

import scala.collection.JavaConverters._

object HttpResponse {
  val DefaultChartset = "UTF-8"

  def apply(bytes: Array[Byte]): HttpResponse = new ByteHttpResponse(bytes)
  def apply(uri: URI, statusLine: String, headers: Map[String, String], payload: Array[Byte]) = new HttpRecord(uri, statusLine, headers, payload)
}

trait HttpResponse extends Serializable {
  def header: HttpHeader
  def payload: Array[Byte]
  def statusLine: String
}

class ByteHttpResponse private[http] (val bytes: Array[Byte]) extends HttpResponse {
  private var _header: HttpHeader = null
  private var _payload: Array[Byte] = null

  lazy val response = {
    var httpResponse: InputStream = null
    try {
      httpResponse = new ByteArrayInputStream(bytes)

      val parser = new HttpResponseParser
      val response = parser.parse(httpResponse)
      val httpHeaders = response.getHeaders

      val header = collection.mutable.Map[String, String]()
      for (httpHeader <- httpHeaders.iterator().asScala) {
        header.put(httpHeader.getName, httpHeader.getValue)
      }
      _header = HttpHeader(header.toMap)

      _payload = IOUtils.toByteArray(httpResponse)

      response
    } finally {
      if (httpResponse != null) httpResponse.close()
    }
  }

  lazy val header = { response; _header }

  lazy val payload = { response; _payload }

  lazy val statusLine = { response; response.getMessage.toString.trim }
}
