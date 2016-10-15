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

package de.l3s.archivespark.http

object HttpHeader {
  val RedirectLocationHeaderField = "Location"
  val RemoteAddrHeaderField = "Remote_Addr"
  val MimeTypeHeaderField = "Content-Type"

  def apply(headers: Map[String, String]): HttpHeader = new HttpHeader(headers)
}

class HttpHeader private (val headers: Map[String, String]) {
  lazy val ip = headers.get(HttpHeader.RemoteAddrHeaderField)

  lazy val mime = headers.get(HttpHeader.MimeTypeHeaderField).map(m => m.split(';').head.trim)

  lazy val mimeTypeParamter = headers.get(HttpHeader.MimeTypeHeaderField).flatMap{m =>
    val split = m.split(';')
    if (split.tail.nonEmpty) {
      val parameterSplit = split(1).split('=')
      Some((parameterSplit(0).trim, if (parameterSplit.tail.nonEmpty) parameterSplit(1).trim else null))
    } else None
  }

  lazy val charset = mimeTypeParamter match {
    case Some((key, value)) => if (key == "charset") Option(value) else None
    case None => None
  }

  lazy val redirectLocation = headers.get(HttpHeader.RedirectLocationHeaderField)
}
