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

abstract class BoundEnrichFuncWithDefaultField[Root <: EnrichRoot, Source, DefaultFieldType](bound: DependentEnrichFunc[Root, _], field: String) extends DependentEnrichFuncWithDefaultField[Root, Source, DefaultFieldType, DefaultFieldType] {
  def this(bound: DependentEnrichFuncWithDefaultField[Root, _, Source, _]) = this(bound, bound.defaultField)

  override def dependency: DependentEnrichFunc[Root, _] = bound
  override def dependencyField: String = field

  override def on(source: Seq[String]): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[DefaultFieldType, DefaultFieldType] = super.on(bound.on(source), dependencyField)
  override def onMulti(source: Seq[String]): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[DefaultFieldType, Seq[DefaultFieldType]] with MultiVal = super.onMulti(bound.on(source), dependencyField)

  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[DefaultFieldType, DefaultFieldType] = super.on(bound.on(dependency, field), dependencyField)
  override def onMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[DefaultFieldType, Seq[DefaultFieldType]] with MultiVal = super.onMulti(bound.on(dependency, field), dependencyField)
}
