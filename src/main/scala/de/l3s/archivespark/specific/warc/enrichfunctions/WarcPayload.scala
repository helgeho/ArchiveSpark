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

package de.l3s.archivespark.specific.warc.enrichfunctions

import de.l3s.archivespark.enrich._
import de.l3s.archivespark.enrich.dataloads.ByteContentLoad
import de.l3s.archivespark.specific.warc.WarcRecord

class WarcPayload private(http: Boolean = true) extends RootEnrichFunc[WarcRecord] with EnrichFuncWithDefaultField[WarcRecord, Any, Array[Byte], Array[Byte]] with DefaultField[Array[Byte]] {
  import WarcPayload._

  override def fields: Seq[String] = if (http) Seq(RecordHeaderField, HttpStatusLineField, HttpHeaderField, PayloadField) else Seq(RecordHeaderField, PayloadField)

  def defaultField: String = PayloadField

  override def aliases = Map(ByteContentLoad.Field -> PayloadField)

  override def deriveRoot(source: WarcRecord, derivatives: Derivatives): Unit = {
    source.access { record =>
      derivatives << record.header
      if (http) {
        derivatives << record.httpResponse.statusLine
        derivatives << record.httpResponse.header.headers
        derivatives << record.httpResponse.payload
      } else {
        derivatives << record.payload
      }
    }
  }
}

object WarcPayload extends WarcPayload(http = true) {
  val RecordHeaderField = "recordHeader"
  val HttpStatusLineField = HttpPayload.StatusLineField
  val HttpHeaderField = HttpPayload.HeaderField
  val PayloadField = "payload"

  def apply(http: Boolean = true) = new WarcPayload(http)
}