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

package de.l3s.archivespark.enrich.functions

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import de.l3s.archivespark.enrich.{Enrichable, Derivatives, EnrichFunc}
import de.l3s.archivespark.utils.IdentityMap
import de.l3s.archivespark.{ArchiveRecordField, ResolvedArchiveRecord}
import org.apache.commons.io.IOUtils
import org.archive.format.http.{HttpHeader, HttpResponseParser}
import org.archive.io.ArchiveReaderFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

object Response extends EnrichFunc[ResolvedArchiveRecord, ResolvedArchiveRecord] {
  override def source: Seq[String] = Seq()
  override def fields = Seq("recordHeader", "httpHeader", "payload")

  override def field: IdentityMap[String] = IdentityMap(
    "content" -> "payload"
  )

  override def derive(source: ResolvedArchiveRecord, derivatives: Derivatives[Enrichable[_]]): Unit = {
    source.access { case (fileName, stream) =>
      val reader = ArchiveReaderFactory.get(fileName, stream, false)
      val record = reader.get
      val header = record.getHeader

      derivatives << ArchiveRecordField(header.getHeaderFields.asScala.toMap)

      var recordOutput: ByteArrayOutputStream = null
      try {
        recordOutput = new ByteArrayOutputStream
        record.dump(recordOutput)
      } finally {
        if (recordOutput != null) recordOutput.close()
      }

      var httpResponse: InputStream = null
      try {
        httpResponse = new ByteArrayInputStream(recordOutput.toByteArray)

        val parser = new HttpResponseParser
        val response = parser.parse(httpResponse)
        val httpHeaders = response.getHeaders

        val httpHeadersMap = mutable.Map[String, String]()
        for (httpHeader: HttpHeader <- httpHeaders.iterator().asScala) {
          httpHeadersMap.put(httpHeader.getName, httpHeader.getValue)
        }

        derivatives << ArchiveRecordField(httpHeadersMap.toMap)

        val payload = IOUtils.toByteArray(httpResponse)

        derivatives << ArchiveRecordField(payload)
      } finally {
        if (httpResponse != null) httpResponse.close()
      }
    }
  }
}