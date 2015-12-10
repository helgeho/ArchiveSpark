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

package de.l3s.archivespark.utils

import java.io.{ByteArrayInputStream, InputStream}

import org.apache.commons.io.IOUtils
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.util.EntityUtils
import org.archive.format.http.{HttpHeader, HttpResponseParser}

import scala.collection.JavaConverters._

object HttpResponse {
  def apply(bytes: Array[Byte]): HttpResponse = new HttpResponse(bytes)
}

class HttpResponse private (bytes: Array[Byte]) {
  private var _header: collection.mutable.Map[String, String] = null
  private var _payload: Array[Byte] = null

  lazy val response = {
    _header = collection.mutable.Map[String, String]()

    var httpResponse: InputStream = null
    try {
      httpResponse = new ByteArrayInputStream(bytes)

      val parser = new HttpResponseParser
      val response = parser.parse(httpResponse)
      val httpHeaders = response.getHeaders

      for (httpHeader: HttpHeader <- httpHeaders.iterator().asScala) {
        _header.put(httpHeader.getName, httpHeader.getValue)
      }

      _payload = IOUtils.toByteArray(httpResponse)

      response
    } finally {
      if (httpResponse != null) httpResponse.close()
    }
  }

  lazy val status = response.getMessage.getStatus

  lazy val header = { response; _header }

  lazy val payload = { response; _payload }

  lazy val stringContent = EntityUtils.toString(new ByteArrayEntity(payload)).trim
}
