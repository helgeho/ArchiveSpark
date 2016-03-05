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

import de.l3s.archivespark.enrich.{DefaultFieldEnrichFunc, Derivatives}
import de.l3s.archivespark.utils.{RawArchiveRecord, IdentityMap}
import de.l3s.archivespark.{ArchiveRecordField, ResolvedArchiveRecord}

class Payload private(http: Boolean = true) extends DefaultFieldEnrichFunc[ResolvedArchiveRecord, ResolvedArchiveRecord, String] {
  import Payload._

  override def fields = if (http) Seq(RecordHeaderField, HttpHeaderField, PayloadField) else Seq(RecordHeaderField, PayloadField)

  def defaultField = PayloadField

  override def field: IdentityMap[String] = IdentityMap(
    "content" -> "payload"
  )

  override def derive(source: ResolvedArchiveRecord, derivatives: Derivatives): Unit = {
    source.access { case (fileName, stream) =>
      val record = RawArchiveRecord(fileName, stream)
      if (record != null) {
        derivatives << ArchiveRecordField(record.header)
        if (http) {
          derivatives << ArchiveRecordField(record.httpResponse.header)
          derivatives << ArchiveRecordField(record.httpResponse.payload)
        } else {
          derivatives << ArchiveRecordField(record.payload)
        }
      }
    }
  }
}

object Payload extends Payload(http = true) {
  val RecordHeaderField = "recordHeader"
  val HttpHeaderField = "httpHeader"
  val PayloadField = "payload"

  def apply(http: Boolean = true) = new Payload(http)
}