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

import java.io.ByteArrayOutputStream

import org.apache.http.entity.ByteArrayEntity
import org.apache.http.util.EntityUtils
import org.archive.io.ArchiveRecord

import scala.collection.JavaConverters._

object HttpArchiveRecord {
  def apply(record: ArchiveRecord): HttpArchiveRecord = new HttpArchiveRecord(record)
}

class HttpArchiveRecord (val record: ArchiveRecord) {
  lazy val header = {
    val header = record.getHeader
    header.getHeaderFields.asScala.mapValues(o => o.toString).toMap
  }

  lazy val httpResponse: HttpResponse = {
    var recordOutput: ByteArrayOutputStream = null
    try {
      recordOutput = new ByteArrayOutputStream()
      record.dump(recordOutput)
      HttpResponse(recordOutput.toByteArray)
    } finally {
      if (recordOutput != null) recordOutput.close()
    }
  }

  lazy val httpHeader = httpResponse.header

  lazy val payload = httpResponse.payload

  lazy val stringContent = EntityUtils.toString(new ByteArrayEntity(payload)).trim

  def close() = record.close()
}