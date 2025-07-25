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

import org.archive.webservices.archivespark.model.pointers.{DependentFieldPointer, FieldPointer, MultiFieldPointer, NamedFieldPointer}

object EnrichFunc {
  type Basic[Root <: EnrichRoot, Source, DefaultValue] = EnrichFunc[Root, EnrichRoot, Source, DefaultValue]
  type Root[Root <: EnrichRoot, Source, DefaultValue] = EnrichFunc[Root, Root, Source, DefaultValue]
  type Any = EnrichFunc[_, _, _, _]
}

trait EnrichFunc[-Root <: UpperRoot, UpperRoot <: EnrichRoot, Source, DefaultValue] extends NamedFieldPointer[Root, DefaultValue] {
  def source: FieldPointer[Root, Source]
  def fields: Seq[String]

  override def dependencyPath: Seq[FieldPointer[Root, _]] = Seq(source) ++ source.dependencyPath

  def defaultField: String = fields.head
  def fieldName: String = defaultField

  def path[R <: Root](root: EnrichRootCompanion[R]): Seq[String] = source.pathTo(root, defaultField)

  def enrich[R <: Root](root: R): R = init(root, excludeFromOutput = false)

  def get[T](field: String) = new DependentFieldPointer[Root, T](this, field)

  override def init[R <: Root](root: R, excludeFromOutput: Boolean): R = source.init(root, excludeFromOutput = true).enrich(source.path(root), this, excludeFromOutput).asInstanceOf[R]

  def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit

  def isEnriched[R <: Root](root: R): Boolean = root(source.path(root)) match {
    case Some(source) => isEnriched(source)
    case None => false
  }

  def isTransparent: Boolean = false

  def isEnriched(enrichable: Enrichable): Boolean = fields.forall(f => enrichable.enrichment(f).isDefined)

  def hasField(name: String): Boolean = fields.contains(name)

  def on[D <: UpperRoot, S <: Source](dependency: FieldPointer[D, S]): EnrichFunc[D, UpperRoot, S, DefaultValue] = {
    val self = this
    new EnrichFunc[D, UpperRoot, S, DefaultValue] {
      override def source: FieldPointer[D, S] = dependency
      override def fields: Seq[String] = self.fields
      override def defaultField: String = self.defaultField
      override def isTransparent: Boolean = self.isTransparent
      override def derive(source: TypedEnrichable[S], derivatives: Derivatives): Unit = self.derive(source, derivatives)
      override def initPartition[R <: EnrichRoot](partition: Iterator[R], init: Iterator[R] => Iterator[R], map: R => R): Iterator[R] = {
        self.initPartition(partition, init, map)
      }
      override def cleanup(): Unit = self.cleanup()
    }
  }

  def of[R <: UpperRoot, S <: Source](dependency: FieldPointer[R, S]): EnrichFunc[R, UpperRoot, S, DefaultValue] = on(dependency)

  def onEach[R <: UpperRoot, S <: Source](dependency: MultiFieldPointer[R, S]): EnrichFunc[R, UpperRoot, S, DefaultValue] = on(dependency.each)
  def ofEach[R <: UpperRoot, S <: Source](dependency: MultiFieldPointer[R, S]): EnrichFunc[R, UpperRoot, S, DefaultValue] = onEach(dependency)

  override def initDependencies[R <: EnrichRoot](partition: Iterator[R], init: R => R): Iterator[R] = {
    initPartition(partition, (iter: Iterator[R]) => source.initDependencies(iter), init)
  }

  override def cleanupDependencies(): Unit = {
    cleanup()
    source.cleanupDependencies()
  }
}