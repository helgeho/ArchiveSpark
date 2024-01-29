/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
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

import org.archive.webservices.archivespark.model.pointers.{FieldPointer, MultiFieldPointer}

trait MultiEnrichFunc[Root <: EnrichRoot, Source, DefaultValue] extends EnrichFunc[Root, Source, Seq[DefaultValue]] with MultiFieldPointer[Root, DefaultValue] {
  override def on[DependencyRoot <: EnrichRoot, S <: Source](dependency: FieldPointer[DependencyRoot, S]): MultiEnrichFunc[DependencyRoot, Source, DefaultValue] = {
    val self = this
    new MultiEnrichFunc[DependencyRoot, Source, DefaultValue] {
      override def source: FieldPointer[DependencyRoot, Source] = dependency.asInstanceOf[FieldPointer[DependencyRoot, Source]]
      override def fields: Seq[String] = self.fields
      override def defaultField: String = self.defaultField
      override def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit = self.derive(source, derivatives)
    }
  }

  override def of[DependencyRoot <: EnrichRoot, S <: Source](source: FieldPointer[DependencyRoot, S]): MultiEnrichFunc[DependencyRoot, Source, DefaultValue] = on(source)

  override def onEach[DependencyRoot <: EnrichRoot, S <: Source](source: MultiFieldPointer[DependencyRoot, S]): MultiEnrichFunc[DependencyRoot, Source, DefaultValue] = on(source.each)
  override def ofEach[DependencyRoot <: EnrichRoot, S <: Source](source: MultiFieldPointer[DependencyRoot, S]): MultiEnrichFunc[DependencyRoot, Source, DefaultValue] = onEach(source)
}