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

package org.archive.archivespark.functions

import org.archive.archivespark.model._
import org.archive.archivespark.model.dataloads.ByteLoad
import org.archive.archivespark.model.pointers.{DataLoadPointer, FieldPointer}
import org.archive.archivespark.sparkling.http.HttpMessage
import org.archive.archivespark.sparkling.util.StringUtil
import org.archive.archivespark.specific.warc.functions.HttpPayload
import org.archive.archivespark.util.HttpHeader

object StringContent extends EnrichFunc[ByteLoad.Root, Array[Byte], String] {
  val DefaultCharset = "UTF-8"

  override def source: FieldPointer[ByteLoad.Root, Array[Byte]] = DataLoadPointer(ByteLoad)
  override def fields: Seq[String] = Seq("string")
  override def derive(source: TypedEnrichable[Array[Byte]], derivatives: Derivatives): Unit = {
    val charsets = source.parent.get[Map[String, String]](HttpPayload.HeaderField) match {
      case Some(headers) => HttpHeader(headers).charset.toSeq ++ HttpMessage.BodyCharsets
      case None => Seq(DefaultCharset)
    }
    derivatives << StringUtil.fromBytes(source.get, charsets).trim
  }
}