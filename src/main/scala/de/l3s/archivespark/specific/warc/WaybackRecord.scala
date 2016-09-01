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

import de.l3s.archivespark.dataspecs.DataEnrichRoot
import de.l3s.archivespark.enrich.RootEnrichFunc
import de.l3s.archivespark.enrich.dataloads.ByteContentLoad
import de.l3s.archivespark.enrich.functions.HttpPayload
import de.l3s.archivespark.http.{HttpClient, HttpRecord}

class WaybackRecord(cdx: CdxRecord) extends DataEnrichRoot[CdxRecord, HttpRecord](cdx) with ByteContentLoad {
  override def access[R >: Null](action: HttpRecord => R): R = {
    HttpClient.get("http://web.archive.org/web/" + cdx.timestamp + "/" + cdx.originalUrl) match {
      case Some(record) => action(record)
      case None => null
    }
  }

  override def defaultEnrichFunction(field: String): Option[RootEnrichFunc[_]] = {
    field match {
      case ByteContentLoad.Field => Some(HttpPayload)
      case _ => None
    }
  }
}

object WaybackRecord {
  implicit def waybackRecordToCdxRecord(record: WaybackRecord): CdxRecord = record.get
}
