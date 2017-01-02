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
import java.security.cert.X509Certificate

import org.apache.commons.compress.utils.IOUtils
import org.apache.http.HttpHost
import org.apache.http.client.entity.{DeflateDecompressingEntity, GzipDecompressingEntity}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpUriRequest}
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.protocol.{BasicHttpContext, HttpCoreContext}
import org.apache.http.ssl.{SSLContexts, TrustStrategy}

import scala.collection.immutable.ListMap
import scala.util.Try

object HttpClient {
  def defaultHeaders = ListMap[String, String](
    "Accept" -> "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
  )

  lazy val client = {
    val sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
      def isTrusted(chain: Array[X509Certificate], authType: String): Boolean = true
    }).build()
    HttpClientBuilder.create().setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext)).build()
  }

  def get(url: String, headers: Map[String, String] = defaultHeaders): Option[HttpRecord] = {
    val uri = URI.create(url)

    val request = new HttpGet(uri)
    for ((k,v) <- headers) request.addHeader(k, v)

    var response: CloseableHttpResponse = null
    var entityStream: InputStream = null
    try {
      val context = new BasicHttpContext()
      response = client.execute(request, context)

      val finalRequest = context.getAttribute(HttpCoreContext.HTTP_REQUEST).asInstanceOf[HttpUriRequest]
      val finalHost = context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST).asInstanceOf[HttpHost]

      val finalUri = URI.create(finalHost.toURI).resolve(finalRequest.getURI)

      val headers = response.getAllHeaders.map(header => header.getName -> header.getValue).toMap

      val responseBytes = new ByteArrayOutputStream()
      val entity = headers.get("X-Archive-Orig-content-encoding") match {
        case Some("gzip") => new GzipDecompressingEntity(response.getEntity)
        case Some("deflate") => new DeflateDecompressingEntity(response.getEntity)
        case _ => response.getEntity
      }
      entityStream = entity.getContent
      IOUtils.copy(entityStream, responseBytes)

      Some(HttpResponse(finalUri, response.getStatusLine.toString.trim, headers, responseBytes.toByteArray))
    } finally {
      if (response != null) Try {response.close()}
      if (entityStream != null) Try {entityStream.close()}
    }
  }
}
