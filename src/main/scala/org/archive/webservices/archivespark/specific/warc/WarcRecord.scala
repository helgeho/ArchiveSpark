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

package org.archive.webservices.archivespark.specific.warc

import java.io.{BufferedInputStream, InputStream}

import org.archive.webservices.archivespark.dataspecs.access.DataAccessor
import org.archive.webservices.archivespark.functions.StringContent
import org.archive.webservices.archivespark.model.dataloads.{ByteLoad, DataLoad, TextLoad}
import org.archive.webservices.archivespark.model.pointers.FieldPointer
import org.archive.webservices.archivespark.model.{DataEnrichRoot, EnrichRootCompanion}
import org.archive.webservices.sparkling.cdx.CdxRecord
import org.archive.webservices.sparkling.warc.{WarcRecord => WARC}
import org.archive.webservices.archivespark.specific.warc.functions.WarcPayload

class WarcRecord(cdx: CdxRecord, val data: DataAccessor[InputStream]) extends DataEnrichRoot[CdxRecord, WARC](cdx) with WarcLikeRecord {
  override def access[R >: Null](action: WARC => R): R = data.access { stream =>
    WARC.get(if (stream.markSupported) stream else new BufferedInputStream(stream)) match {
      case Some(record) => action(record)
      case None => null
    }
  }

  override def companion: EnrichRootCompanion[WarcRecord] = WarcRecord
}

object WarcRecord extends EnrichRootCompanion[WarcRecord] {
  override def dataLoad[T](load: DataLoad[T]): Option[FieldPointer[WarcRecord, T]] = (load match {
    case ByteLoad => Some(WarcPayload)
    case TextLoad => Some(StringContent)
    case _ => None
  }).map(_.asInstanceOf[FieldPointer[WarcRecord, T]])
}