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

class Values private (override val resultField: String, funcs: Seq[EnrichFunc[_, _] with DefaultFieldAccess[_, _]]) extends EnrichFuncWithDefaultField[EnrichRoot, Any, Array[_], Array[_]] with SingleField[Array[_]] {
  override def source: Seq[String] = Seq.empty

  override protected[enrich] def derive(source: TypedEnrichable[Any], derivatives: Derivatives): Unit = {
    val chain = source.chain
    val values = for (func <- funcs) yield {
      var commonParent = chain.head
      val valueSuffix = func.source.zipWithIndex.dropWhile{case (s, i) =>
        if (i + 1 < chain.size) {
          val field = commonParent.field(s)
          val next = chain(i + 1)
          if (next.field == field || (field == "*" && next.field.matches("\\[\\d+\\]"))) {
            commonParent = next
            true
          } else false
        } else false
      }.map(_._1)
      commonParent(valueSuffix).flatMap(_.get[Any](func.defaultField)).get
    }
    derivatives << values.toArray
  }
}

object Values {
  def apply(resultField: String, funcs: (EnrichFunc[_, _] with DefaultFieldAccess[_, _])*): Values = new Values(resultField, funcs)
}