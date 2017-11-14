/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2017 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.enrich.functions

import de.l3s.archivespark.enrich._

class Values[Root <: EnrichRoot, Source] private (override val resultField: String, funcs: Seq[EnrichFunc[_, _] with DefaultFieldAccess[_, _]]) extends EnrichFunc[Root, Source] with SingleField[Array[_]] {
  override def source: Seq[String] = Seq.empty

  override protected[enrich] def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit = {
    val values = for (func <- funcs) yield {
      var sourcePath = collection.mutable.Seq(this.source: _*)
      val valueSuffix = func.source.dropWhile(sourcePath.drop(1).headOption.contains)
      val commonLength = func.source.size - valueSuffix.size
      val commonParentUpLength = this.source.size - commonLength
      var commonParent = source
      for (up <- 1 to commonParentUpLength) commonParent = commonParent.parent
      commonParent.get(valueSuffix).get
    }
    derivatives << values.toArray
  }
}

object Values {
  def apply[Root <: EnrichRoot, Source](resultField: String, funcs: (EnrichFunc[_, _] with DefaultFieldAccess[_, _])*): Values[Root, Source] = new Values(resultField, funcs)
}