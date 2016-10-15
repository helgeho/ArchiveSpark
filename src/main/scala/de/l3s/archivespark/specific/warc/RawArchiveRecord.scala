/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.specific.warc

import java.io.{ByteArrayOutputStream, InputStream}

import de.l3s.archivespark.http.HttpResponse
import org.archive.io.arc.ARCReaderFactory
import org.archive.io.warc.WARCReaderFactory
import org.archive.io.{ArchiveReader, ArchiveRecord}

import scala.collection.JavaConverters._
import scala.util.Try

object RawArchiveRecord {
  def apply(filename: String, stream: InputStream): RawArchiveRecord = {
    var reader: ArchiveReader = null
    var record: RawArchiveRecord = null
    try {
      val isArc = ARCReaderFactory.isARCSuffix(filename)
      reader = if (isArc) ARCReaderFactory.get(filename, stream, false) else WARCReaderFactory.get(filename, stream, false)
      record = new RawArchiveRecord(reader.get)
    } finally {
      if (reader != null) Try{reader.close()}
    }
    record
  }
}

class RawArchiveRecord private (val record: ArchiveRecord) {
  val header = {
    val header = record.getHeader
    Try { header.getHeaderFields.asScala.mapValues(o => o.toString).toMap }.getOrElse(Map.empty)
  }

  val payload: Array[Byte] = {
    var recordOutput: ByteArrayOutputStream = new ByteArrayOutputStream()
    try {
      record.dump(recordOutput)
      recordOutput.toByteArray
    } finally {
      recordOutput.close()
    }
  }

  lazy val httpResponse: HttpResponse = HttpResponse(payload)
}