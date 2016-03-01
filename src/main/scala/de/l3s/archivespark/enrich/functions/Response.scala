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

import de.l3s.archivespark.enrich.{DefaultFieldEnrichFunc, Derivatives, EnrichFunc, Enrichable}
import de.l3s.archivespark.utils.{HttpArchiveRecord, IdentityMap}
import de.l3s.archivespark.{ArchiveRecordField, ResolvedArchiveRecord}
import org.archive.io.ArchiveReaderFactory

object Response extends DefaultFieldEnrichFunc[ResolvedArchiveRecord, ResolvedArchiveRecord, String] {
  val RecordHeaderField = "recordHeader"
  val HttpHeaderField = "httpHeader"
  val PayloadField = "payload"

  override def fields = Seq(RecordHeaderField, HttpHeaderField, PayloadField)

  def defaultField = PayloadField

  override def field: IdentityMap[String] = IdentityMap(
    "content" -> "payload"
  )

  override def derive(source: ResolvedArchiveRecord, derivatives: Derivatives): Unit = {
    source.access { case (fileName, stream) =>
      val reader = ArchiveReaderFactory.get(fileName, stream, false)
      val record = HttpArchiveRecord(reader.get)

      derivatives << ArchiveRecordField(record.header)
      derivatives << ArchiveRecordField(record.httpHeader.headers)
      derivatives << ArchiveRecordField(record.payload)

      record.close()
    }
  }
}