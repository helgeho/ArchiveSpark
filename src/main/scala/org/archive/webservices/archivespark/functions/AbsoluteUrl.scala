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

import java.net.URL

import org.archive.webservices.archivespark.model._
import org.archive.webservices.archivespark.model.dataloads.TextLoad
import org.archive.webservices.archivespark.model.pointers.DataLoadPointer
import org.archive.webservices.sparkling.cdx.CdxRecord

import scala.util.Try

object AbsoluteUrl extends EnrichFunc[TextLoad.Root, String, String] with BasicEnrichFunc[TextLoad.Root, String, String] {
  val func: EnrichFunc[TextLoad.Root, String, String] = DataLoadPointer(TextLoad).mapEnrichable("absolute") { enrichable =>
    Try{enrichable.root[CdxRecord].get.originalUrl}.toOption match {
      case Some(baseUrl) => new URL(new URL(enrichable.get), baseUrl).toString
      case None => enrichable.get
    }
  }
}