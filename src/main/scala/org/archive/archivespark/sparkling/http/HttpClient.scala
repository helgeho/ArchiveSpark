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
import java.net.{HttpURLConnection, URL, URLConnection}

import org.archive.archivespark.sparkling.logging.LogContext
import org.archive.archivespark.sparkling.util.Common

import scala.collection.JavaConverters._
import scala.util.Try

object HttpClient {
  val DefaultRetries: Int = 30
  val DefaultSleepMillis: Int = 1000
  val DefaultTimeoutMillis: Int = -1

  implicit val logContext: LogContext = LogContext(this)

  def request[R](url: String, headers: Map[String, String] = Map.empty, retries: Int = DefaultRetries, sleepMillis: Int = DefaultSleepMillis, timeoutMillis: Int = DefaultTimeoutMillis)(action: InputStream => R): R = rangeRequest(url, headers, retries = retries, sleepMillis = sleepMillis, timeoutMillis = timeoutMillis)(action)

  def rangeRequest[R](url: String, headers: Map[String, String] = Map.empty, offset: Long = 0, length: Long = -1, retries: Int = DefaultRetries, sleepMillis: Int = DefaultSleepMillis, timeoutMillis: Int = DefaultTimeoutMillis)(action: InputStream => R): R = {
    rangeRequestConnection(url, headers, offset, length, retries, sleepMillis, timeoutMillis) { case connection: HttpURLConnection =>
      val in = new BufferedInputStream(connection.getInputStream)
      val r = action(in)
      Try(in.close())
      r
    }
  }

  def requestMessage[R](url: String, headers: Map[String, String] = Map.empty, retries: Int = DefaultRetries, sleepMillis: Int = DefaultSleepMillis, timeoutMillis: Int = DefaultTimeoutMillis)(action: HttpMessage => R): R = rangeRequestMessage(url, headers, retries = retries, sleepMillis = sleepMillis, timeoutMillis = timeoutMillis)(action)

  def rangeRequestMessage[R](url: String, headers: Map[String, String] = Map.empty, offset: Long = 0, length: Long = -1, retries: Int = DefaultRetries, sleepMillis: Int = DefaultSleepMillis, timeoutMillis: Int = DefaultTimeoutMillis)(action: HttpMessage => R): R = {
    rangeRequestConnection(url, headers, offset, length, retries, sleepMillis, timeoutMillis) { case connection: HttpURLConnection =>
      val in = new BufferedInputStream(connection.getInputStream)
      val responseHeaders = connection.getHeaderFields.asScala.toMap.flatMap{case (k, v) => v.asScala.headOption.map(k -> _)}
      val message = new HttpMessage(connection.getResponseMessage, responseHeaders, in)
      val r = action(message)
      Try(in.close())
      r
    }
  }

  def requestConnection[R](url: String, headers: Map[String, String] = Map.empty, retries: Int = DefaultRetries, sleepMillis: Int = DefaultSleepMillis, timeoutMillis: Int = DefaultTimeoutMillis)(action: URLConnection => R): R = rangeRequestConnection(url, headers, retries = retries, sleepMillis = sleepMillis, timeoutMillis = timeoutMillis)(action)

  def rangeRequestConnection[R](url: String, headers: Map[String, String] = Map.empty, offset: Long = 0, length: Long = -1, retries: Int = DefaultRetries, sleepMillis: Int = DefaultSleepMillis, timeoutMillis: Int = DefaultTimeoutMillis)(action: URLConnection => R): R = {
    Common.timeoutWithReporter(timeoutMillis) { reporter =>
      val connection = Common.retry(retries, sleepMillis, (retry, e) => {
        "Request failed (" + retry + "/" + retries + "): " + url + " (" + offset + "-" + (if (length >= 0) length else "") + ") - " + e.getMessage
      }) { _ =>
        reporter.alive()
        val connection = new URL(url).openConnection()
        for ((key, value) <- headers) connection.addRequestProperty(key, value)
        if (offset > 0 || length >= 0) connection.addRequestProperty("Range", "bytes=" + offset + "-" + (if (length >= 0) offset + length - 1 else ""))
        connection.asInstanceOf[HttpURLConnection]
      }
      val r = action(connection)
      Try(connection.disconnect())
      r
    }
  }
}

