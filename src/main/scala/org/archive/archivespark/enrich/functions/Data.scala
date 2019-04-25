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

package org.archive.archivespark.enrich.functions

import org.archive.archivespark.dataspecs.DataEnrichRoot
import org.archive.archivespark.enrich._

class Data[T] (field: String, alias: Option[String] = None) extends EnrichFuncWithDefaultField[DataEnrichRoot[_, _], Any, T, T] with RootEnrichFunc[DataEnrichRoot[_, _]] with SingleField[T] {
  override def fields: Seq[String] = Seq(field)

  override def aliases: Map[String, String] = alias.map(_ -> field).toMap

  override def deriveRoot(source: DataEnrichRoot[_, _], derivatives: Derivatives): Unit = {
    source.access { data =>
      derivatives << data
    }
  }
}

object Data extends Data[Any]("data", None) {
  def apply[T](field: String): Data[T] = new Data[T](field)
  def apply[T](field: String, alias: String): Data[T] = new Data[T](field, Some(alias))
}