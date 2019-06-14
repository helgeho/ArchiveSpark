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
import org.archive.archivespark.model.pointers.{FieldPointer, NamedFieldPointer}

class Values[R <: EnrichRoot] private (field: String, pointers: Seq[FieldPointer[R, _]], defaultValues: Seq[Option[_]]) extends EnrichFunc[R, Any, Array[_]] {
  override def fields: Seq[String] = Seq(field)

  override def source: FieldPointer[R, Any] = FieldPointer.root[TypedEnrichRoot[Any], Any].asInstanceOf[FieldPointer[R, Any]]

  override def derive(source: TypedEnrichable[Any], derivatives: Derivatives): Unit = {
    val chain = source.chain
    val values = for ((pointer, idx) <- pointers.zipWithIndex) yield {
      try {
        var commonParent = chain.head
        val valueSuffix = pointer.path(source.root[Any].companion.asInstanceOf[EnrichRootCompanion[R]]).zipWithIndex.dropWhile { case (f, i) =>
          if (i + 1 < chain.size) {
            val next = chain(i + 1)
            if (next.field == f || (f == "*" && next.field.matches("\\[\\d+\\]"))) {
              commonParent = next
              true
            } else false
          } else false
        }.map(_._1)
        commonParent(valueSuffix).get
      } catch {
        case e: Exception =>
          if (idx < defaultValues.length && defaultValues(idx).isDefined) defaultValues(idx).get
          else throw e
      }
    }
    derivatives << values.toArray
  }
}

object Values {
  def apply[R <: EnrichRoot](resultField: String, pointers: FieldPointer[R, _]*): Values[R] = new Values(resultField, pointers, Seq.empty)
  def withDefaults[R <: EnrichRoot](resultField: String, pointers: FieldPointer[R, _]*)(defaultValues: Option[_]*): Values[R] = new Values(resultField, pointers, defaultValues)

  def apply[R <: EnrichRoot](pointers: NamedFieldPointer[R, _]*): Values[R] = {
    val resultField = pointers.map(_.fieldName).mkString("_")
    new Values(resultField, pointers, Seq.empty)
  }

  def withDefaults[R <: EnrichRoot](pointers: NamedFieldPointer[R, _]*)(defaultValues: Option[_]*): Values[R] = {
    val resultField = pointers.map(_.fieldName).mkString("_")
    new Values(resultField, pointers, defaultValues)
  }
}