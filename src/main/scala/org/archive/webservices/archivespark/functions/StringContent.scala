/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2024 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.functions

import org.apache.commons.io.input.BoundedInputStream
import org.archive.webservices.archivespark.model._
import org.archive.webservices.archivespark.model.dataloads.ByteLoad
import org.archive.webservices.archivespark.model.pointers.{DataLoadPointer, FieldPointer}
import org.archive.webservices.sparkling.http.HttpMessage
import org.archive.webservices.sparkling.util.StringUtil
import org.archive.webservices.archivespark.specific.warc.functions.HttpPayload
import org.archive.webservices.archivespark.util.{Bytes, HttpHeader}
import org.archive.webservices.sparkling._
import org.archive.webservices.sparkling.html.HtmlProcessor

object StringContent extends EnrichFunc.Basic[ByteLoad.Root, Bytes, String] {
  val MaxContentLength: Long = 1.mb
  val DefaultCharset = "UTF-8"

  override def source: FieldPointer[ByteLoad.Root, Bytes] = DataLoadPointer(ByteLoad)
  override def fields: Seq[String] = Seq("string")
  override def derive(source: TypedEnrichable[Bytes], derivatives: Derivatives): Unit = {
    val (mime, charsets) = source.parent.get[Seq[(String, String)]](HttpPayload.HeaderField) match {
      case Some(headers) =>
        val h = HttpHeader(headers)
        (h.mime, h.charset.toSeq ++ HttpMessage.BodyCharsets)
      case None => (None, Seq(DefaultCharset))
    }
    derivatives << {
      if (mime.map(_.toLowerCase).contains("text/html")) {
        val in = new BoundedInputStream(source.get.stream, MaxContentLength)
        try {
          HtmlProcessor.readStream(in, charsets).trim
        } finally {
          in.close()
        }
      } else {
        StringUtil.fromBytes(source.get.array(MaxContentLength, maxLength = true), charsets).trim
      }
    }
  }
}