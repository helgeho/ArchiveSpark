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

package org.archive.archivespark.specific.warc

import org.archive.archivespark.functions.StringContent
import org.archive.archivespark.model.dataloads.{ByteLoad, DataLoad, TextLoad}
import org.archive.archivespark.model.pointers.FieldPointer
import org.archive.archivespark.model.{DataEnrichRoot, EnrichRootCompanion}
import org.archive.archivespark.sparkling.cdx.CdxRecord
import org.archive.archivespark.sparkling.http.{HttpClient, HttpMessage}
import org.archive.archivespark.specific.warc.functions.HttpPayload

class WaybackRecord(cdx: CdxRecord) extends DataEnrichRoot[CdxRecord, HttpMessage](cdx) with WarcLikeRecord {
  val WaybackUrl = "http://web.archive.org/web/$timestampid_/$url"

  def waybackUrl(timestamp: String, url: String): String = {
    WaybackUrl.replace("$timestamp", timestamp).replace("$url", url)
  }

  override def access[R >: Null](action: HttpMessage => R): R = {
    val url = waybackUrl(cdx.timestamp, cdx.originalUrl)
    HttpClient.requestMessage(url)(msg => action(msg))
  }

  override def companion: EnrichRootCompanion[WaybackRecord] = WaybackRecord
}

object WaybackRecord extends EnrichRootCompanion[WaybackRecord] {
  override def dataLoad[T](load: DataLoad[T]): Option[FieldPointer[WaybackRecord, T]] = (load match {
    case ByteLoad => Some(HttpPayload)
    case TextLoad => Some(StringContent)
    case _ => None
  }).map(_.asInstanceOf[FieldPointer[WaybackRecord, T]])
}