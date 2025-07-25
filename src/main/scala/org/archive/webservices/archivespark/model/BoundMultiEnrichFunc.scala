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

import org.archive.webservices.archivespark.model.pointers.{DependentFieldPointer, FieldPointer}

abstract class BoundMultiEnrichFunc[-Root <: UpperRoot, UpperRoot <: EnrichRoot, Source, Bound, DefaultValue](bound: EnrichFunc[Root, UpperRoot, Source, Bound]) extends MultiEnrichFunc[Root, UpperRoot, Source, DefaultValue] {
  def source: FieldPointer[Root, Source] = bound.source

  override def on[D <: UpperRoot, S <: Source](dependency: FieldPointer[D, S]): MultiEnrichFunc[D, UpperRoot, S, DefaultValue] = {
    val self = this
    new BoundMultiEnrichFunc[D, UpperRoot, S, Bound, DefaultValue](bound.on(dependency)) {
      override def fields: Seq[String] = self.fields
      override def isTransparent: Boolean = self.isTransparent
      override def deriveBound(source: TypedEnrichable[Bound], derivatives: Derivatives): Unit = {
        self.deriveBound(source, derivatives)
      }
      override def initPartition[R <: EnrichRoot](partition: Iterator[R], init: Iterator[R] => Iterator[R], map: R => R): Iterator[R] = {
        self.initPartition(partition, init, map)
      }
      override def cleanup(): Unit = self.cleanup()
    }
  }

  private lazy val func = {
    val self = this
    new MultiEnrichFunc[Root, UpperRoot, Bound, DefaultValue] {
      override def source: FieldPointer[Root, Bound] = bound
      override def fields: Seq[String] = self.fields
      override def isTransparent: Boolean = self.isTransparent
      override def derive(source: TypedEnrichable[Bound], derivatives: Derivatives): Unit = {
        deriveBound(source, derivatives)
      }
      override def initPartition[R <: EnrichRoot](partition: Iterator[R], init: Iterator[R] => Iterator[R], map: R => R): Iterator[R] = {
        self.initPartition(partition, init, map)
      }
      override def cleanup(): Unit = self.cleanup()
    }
  }

  override def dependencyPath: Seq[FieldPointer[Root, _]] = func.dependencyPath

  override def path[R <: Root](root: EnrichRootCompanion[R]): Seq[String] = func.path(root)

  override def get[T](field: String): DependentFieldPointer[Root, T] = func.get(field)

  override def isEnriched[R <: Root](root: R): Boolean = func.isEnriched(root)

  override def isEnriched(enrichable: Enrichable): Boolean = func.isEnriched(enrichable)

  override def init[R <: Root](root: R, excludeFromOutput: Boolean): R = {
    func.init(root, excludeFromOutput)
  }

  override def enrich[R <: Root](root: R): R = func.enrich(root)

  override def initDependencies[R <: EnrichRoot](partition: Iterator[R], init: R => R): Iterator[R] = {
    func.initDependencies(partition, init)
  }

  override def cleanupDependencies(): Unit = {
    func.cleanupDependencies()
  }

  override def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit = {
    throw new RuntimeException("this should never be called in a bound function")
  }

  def deriveBound(source: TypedEnrichable[Bound], derivatives: Derivatives): Unit
}