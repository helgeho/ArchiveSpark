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

package org.archive.webservices.archivespark.model

import org.archive.webservices.archivespark.model.pointers.FieldPointer

abstract class BoundMultiEnrichFunc[-Root <: UpperRoot, UpperRoot <: EnrichRoot, Source, Bound, DefaultValue](bound: EnrichFunc[Root, UpperRoot, Source, Bound]) extends MultiEnrichFunc[Root, UpperRoot, Source, DefaultValue] {
  def source: FieldPointer[Root, Source] = bound.source

  override def on[D <: UpperRoot, S <: Source](dependency: FieldPointer[D, S]): MultiEnrichFunc[D, UpperRoot, S, DefaultValue] = {
    val self = this
    new BoundMultiEnrichFunc[D, UpperRoot, S, Bound, DefaultValue](bound.on(dependency)) {
      override def fields: Seq[String] = self.fields
      override def defaultField: String = self.defaultField
      override def deriveBound(source: TypedEnrichable[Bound], derivatives: Derivatives): Unit = {
        self.deriveBound(source, derivatives)
      }
      override def initPartition[R <: EnrichRoot](partition: Iterator[R], init: R => R): Iterator[R] = {
        self.initPartition(partition, init)
      }
      override def cleanup(): Unit = self.cleanup()
    }
  }

  override def init[R <: Root](root: R, excludeFromOutput: Boolean): R = {
    source.init(root, excludeFromOutput = true).enrich(source.path(root), bound, excludeFromOutput = true).enrich(bound.path(root), this, excludeFromOutput).asInstanceOf[R]
  }

  override def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit = {
    deriveBound(source.enrichment(bound.defaultField).get, derivatives)
  }

  def deriveBound(source: TypedEnrichable[Bound], derivatives: Derivatives): Unit
}