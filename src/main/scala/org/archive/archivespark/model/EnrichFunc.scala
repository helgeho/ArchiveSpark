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

package org.archive.archivespark.model

import org.archive.archivespark.model.pointers.{DependentFieldPointer, FieldPointer, MultiFieldPointer, NamedFieldPointer}

trait EnrichFunc[Root <: EnrichRoot, Source, DefaultValue] extends NamedFieldPointer[Root, DefaultValue] {
  def source: FieldPointer[Root, Source]
  def fields: Seq[String]

  def defaultField: String = fields.head
  def fieldName: String = defaultField

  def path[R <: Root](root: EnrichRootCompanion[R]): Seq[String] = source.pathTo(root, defaultField)

  def enrich[R <: Root](root: R): R = init(root, excludeFromOutput = false)

  def get[T](field: String) = new DependentFieldPointer[Root, T](this, field)

  override def init[R <: Root](root: R, excludeFromOutput: Boolean): R = source.init(root, excludeFromOutput = true).enrich(source.path(root), this, excludeFromOutput).asInstanceOf[R]

  def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit

  def isEnriched(root: Root): Boolean = root(source.path(root)) match {
    case Some(source) => isEnriched(root)
    case None => false
  }

  def isEnriched(enrichable: Enrichable): Boolean = fields.forall(f => enrichable.enrichment(f).isDefined)

  def hasField(name: String): Boolean = fields.contains(name)

  def on[DependencyRoot <: EnrichRoot](dependency: FieldPointer[DependencyRoot, Source]): EnrichFunc[DependencyRoot, Source, DefaultValue] = {
    val self = this
    new EnrichFunc[DependencyRoot, Source, DefaultValue] {
      override def source: FieldPointer[DependencyRoot, Source] = dependency
      override def fields: Seq[String] = self.fields
      override def defaultField: String = self.defaultField
      override def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit = self.derive(source, derivatives)
    }
  }

  def of[DependencyRoot <: EnrichRoot](dependency: FieldPointer[DependencyRoot, Source]): EnrichFunc[DependencyRoot, Source, DefaultValue] = on(dependency)

  def onEach[DependencyRoot <: EnrichRoot](dependency: MultiFieldPointer[DependencyRoot, Source]): EnrichFunc[DependencyRoot, Source, DefaultValue] = on(dependency.each)
  def ofEach[DependencyRoot <: EnrichRoot](dependency: MultiFieldPointer[DependencyRoot, Source]): EnrichFunc[DependencyRoot, Source, DefaultValue] = onEach(dependency)
}