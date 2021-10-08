package org.archive.archivespark.sparkling.http

import java.io.{BufferedInputStream, InputStream}
import java.net.{HttpURLConnection, URL, URLConnection}

import org.archive.archivespark.sparkling.io.CleanupInputStream
import org.archive.archivespark.sparkling.logging.LogContext
import org.archive.archivespark.sparkling.util.Common

import scala.collection.JavaConverters._
import scala.util.Try

object HttpClient {
  val DefaultRetries: Int = 30
  val DefaultSleepMillis: Int = 1000
  val DefaultTimeoutMillis: Int = -1

  implicit val logContext: LogContext = LogContext(this)

  def request[R](url: String,
                 headers: Map[String, String] = Map.empty,
                 retries: Int = DefaultRetries,
                 sleepMillis: Int = DefaultSleepMillis,
                 timeoutMillis: Int = DefaultTimeoutMillis,
                 close: Boolean = true)(action: InputStream => R): R =
    rangeRequest(
      url,
      headers,
      retries = retries,
      sleepMillis = sleepMillis,
      timeoutMillis = timeoutMillis,
      close = close
    )(action)

  def rangeRequest[R](url: String,
                      headers: Map[String, String] = Map.empty,
                      offset: Long = 0,
                      length: Long = -1,
                      retries: Int = DefaultRetries,
                      sleepMillis: Int = DefaultSleepMillis,
                      timeoutMillis: Int = DefaultTimeoutMillis,
                      close: Boolean = true)(action: InputStream => R): R = {
    rangeRequestConnection(
      url,
      headers,
      offset,
      length,
      retries,
      sleepMillis,
      timeoutMillis,
      disconnect = false
    ) {
      case connection: HttpURLConnection =>
        val in = new CleanupInputStream(
          new BufferedInputStream(connection.getInputStream),
          connection.disconnect
        )
        Common.cleanup(action(in))(() => if (close) in.close())
    }
  }

  def requestMessage[R](url: String,
                        headers: Map[String, String] = Map.empty,
                        retries: Int = DefaultRetries,
                        sleepMillis: Int = DefaultSleepMillis,
                        timeoutMillis: Int = DefaultTimeoutMillis,
                        close: Boolean = true)(action: HttpMessage => R): R =
    rangeRequestMessage(
      url,
      headers,
      retries = retries,
      sleepMillis = sleepMillis,
      timeoutMillis = timeoutMillis,
      close = close
    )(action)

  def rangeRequestMessage[R](
    url: String,
    headers: Map[String, String] = Map.empty,
    offset: Long = 0,
    length: Long = -1,
    retries: Int = DefaultRetries,
    sleepMillis: Int = DefaultSleepMillis,
    timeoutMillis: Int = DefaultTimeoutMillis,
    close: Boolean = true
  )(action: HttpMessage => R): R = {
    rangeRequestConnection(
      url,
      headers,
      offset,
      length,
      retries,
      sleepMillis,
      timeoutMillis,
      disconnect = false
    ) {
      case connection: HttpURLConnection =>
        val in = new CleanupInputStream(
          new BufferedInputStream(connection.getInputStream),
          connection.disconnect
        )
        Common.cleanup({
          val responseHeaders =
            connection.getHeaderFields.asScala.toSeq.flatMap {
              case (k, v) => v.asScala.map((if (k == null) "" else k) -> _)
            }
          val message =
            new HttpMessage(connection.getHeaderField(0), responseHeaders, in)
          action(message)
        })(() => if (close) in.close())
    }
  }

  def requestConnection[R](
    url: String,
    headers: Map[String, String] = Map.empty,
    retries: Int = DefaultRetries,
    sleepMillis: Int = DefaultSleepMillis,
    timeoutMillis: Int = DefaultTimeoutMillis,
    disconnect: Boolean = true
  )(action: URLConnection => R): R =
    rangeRequestConnection(
      url,
      headers,
      retries = retries,
      sleepMillis = sleepMillis,
      timeoutMillis = timeoutMillis,
      disconnect = disconnect
    )(action)

  def rangeRequestConnection[R](
    url: String,
    headers: Map[String, String] = Map.empty,
    offset: Long = 0,
    length: Long = -1,
    retries: Int = DefaultRetries,
    sleepMillis: Int = DefaultSleepMillis,
    timeoutMillis: Int = DefaultTimeoutMillis,
    disconnect: Boolean = true
  )(action: URLConnection => R): R = {
    val connection = Common.retryObj(
      new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    )(
      retries,
      sleepMillis,
      _.disconnect,
      (_, retry, e) => {
        "Request failed (" + retry + "/" + retries + "): " + url + " (" + offset + "-" + (if (length >= 0)
                                                                                            length
                                                                                          else
                                                                                            "") + ") - " + e.getClass.getCanonicalName + Option(
          e.getMessage
        ).map(_.trim).filter(_.nonEmpty).map(" - " + _).getOrElse("")
      }
    ) { (connection, _) =>
      if (timeoutMillis >= 0) connection.setConnectTimeout(timeoutMillis)
      for ((key, value) <- headers) connection.addRequestProperty(key, value)
      if (offset > 0 || length >= 0)
        connection.addRequestProperty(
          "Range",
          "bytes=" + offset + "-" + (if (length >= 0) offset + length - 1
                                     else "")
        )
      connection.setInstanceFollowRedirects(false)
      val redirect =
        if (connection.getResponseCode / 100 == 3)
          Option(connection.getHeaderField("Location"))
        else None
      if (redirect.isDefined) {
        Try(connection.disconnect())
        return rangeRequestConnection(
          redirect.get,
          headers,
          offset,
          length,
          retries,
          sleepMillis,
          timeoutMillis,
          disconnect
        )(action)
      }
      connection
    }
    if (disconnect) Common.cleanup(action(connection))(connection.disconnect)
    else action(connection)
  }
}
