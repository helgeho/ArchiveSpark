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

package de.l3s.archivespark.enrich

trait DependentEnrichFuncWithDefaultField[Root <: EnrichRoot, Source, InternalFieldType, ExternalFieldType] extends DependentEnrichFunc[Root, Source] with EnrichFuncWithDefaultField[Root, Source, InternalFieldType, ExternalFieldType] {
  override def onMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = {
    onMulti(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField)
  }

  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal, index: Int)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, index)
  override def onMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = {
    onMulti(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField, index)
  }

  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = {
    on(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField)
  }
  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = {
    on(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField, index)
  }
  override def onEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = {
    onEach(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField)
  }
}
