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

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.URI

import org.apache.commons.compress.utils.IOUtils
import org.apache.http.HttpHost
import org.apache.http.client.methods.{HttpGet, HttpUriRequest}
import org.apache.http.impl.client.SystemDefaultHttpClient
import org.apache.http.protocol.{BasicHttpContext, ExecutionContext}

import scala.collection.immutable.ListMap
import scala.util.Try

object HttpClient {
  def defaultHeaders = ListMap[String, String](
    "Accept" -> "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
  )

  lazy val client = new SystemDefaultHttpClient()

  def get(url: String, headers: Map[String, String] = defaultHeaders): Option[HttpRecord] = {
    val uri = URI.create(url)

    val request = new HttpGet(uri)
    for ((k,v) <- headers) request.addHeader(k, v)

    var entityStream: InputStream = null
    try {
      val context = new BasicHttpContext()
      val response = client.execute(request, context)

      val finalRequest = context.getAttribute(ExecutionContext.HTTP_REQUEST).asInstanceOf[HttpUriRequest]
      val finalHost = context.getAttribute(ExecutionContext.HTTP_TARGET_HOST).asInstanceOf[HttpHost]

      val finalUri = URI.create(finalHost.toURI).resolve(finalRequest.getURI)

      val headers = response.getAllHeaders.map(header => header.getName -> header.getValue).toMap

      val responseBytes = new ByteArrayOutputStream()
      val entity = response.getEntity
      entityStream = entity.getContent
      try {
        IOUtils.copy(entityStream, responseBytes)
      } catch {
        case e: Exception => e.printStackTrace()
      }

      Some(HttpResponse(finalUri, response.getStatusLine.toString.trim, headers, responseBytes.toByteArray))
    } finally {
      if (entityStream != null) Try {entityStream.close()}
    }
  }
}
