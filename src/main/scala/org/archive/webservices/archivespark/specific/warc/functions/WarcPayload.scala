/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.specific.warc.functions

import org.archive.webservices.archivespark.model._
import org.archive.webservices.archivespark.model.pointers.FieldPointer
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.warc.WarcRecord

class WarcPayload private (http: Boolean = true)
    extends EnrichFunc[DataEnrichRoot[Any, WarcRecord], Any, Array[Byte]] {
  import WarcPayloadFields._

  val source: FieldPointer[DataEnrichRoot[Any, WarcRecord], Any] =
    FieldPointer.root[DataEnrichRoot[Any, WarcRecord], Any]

  val fields: Seq[String] =
    if (http) Seq(RecordHeader, HttpStatusLine, HttpHeader, Payload)
    else Seq(RecordHeader, Payload)

  override val defaultField: String = Payload

  override def derive(source: TypedEnrichable[Any],
                      derivatives: Derivatives): Unit = {
    source.asInstanceOf[DataEnrichRoot[Any, WarcRecord]].access { record =>
      derivatives << record.headers.toMap
      if (http) {
        for (msg <- record.http) {
          derivatives << msg.statusLine
          derivatives << msg.headers
          derivatives << IOUtil.bytes(msg.payload)
        }
      } else {
        derivatives << IOUtil.bytes(record.payload)
      }
    }
  }
}

object WarcPayloadFields {
  val RecordHeader: String = "recordHeader"
  val HttpStatusLine: String = HttpPayload.StatusLineField
  val HttpHeader: String = HttpPayload.HeaderField
  val Payload: String = "payload"
}

object WarcPayload extends WarcPayload(http = true) {
  def apply(http: Boolean = true) = new WarcPayload(http)
}
