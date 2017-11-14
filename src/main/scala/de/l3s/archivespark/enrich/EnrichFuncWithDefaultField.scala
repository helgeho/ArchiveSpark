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

import de.l3s.archivespark.utils.SelectorUtil

trait EnrichFuncWithDefaultField[Root <: EnrichRoot, Source, InternalFieldType, ExternalFieldType] extends EnrichFunc[Root, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] {
  override def onRoot: EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(Seq.empty)
  override def ofRoot: EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = onRoot

  override def on(source: Seq[String]): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = new PipedEnrichFuncWithDefaultField[Source, InternalFieldType, ExternalFieldType](this, source)

  def onMulti(source: Seq[String]): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = new MultiPipedEnrichFuncWithDefaultField[Source, InternalFieldType, ExternalFieldType](this, source)
  override def onEach(source: Seq[String]): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(source :+ "*")

  def onMulti(source: String): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(SelectorUtil.parse(source))
  override def on(source: String): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(SelectorUtil.parse(source))

  def onMulti(source: Seq[String], index: Int): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(source :+ s"[$index]")
  override def on(source: Seq[String], index: Int): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(source :+ s"[$index]")

  def onMulti(source: String, index: Int): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(SelectorUtil.parse(source), index)
  override def on(source: String, index: Int): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(SelectorUtil.parse(source), index)

  override def onEach(source: String): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onEach(SelectorUtil.parse(source))

  def ofMulti(source: String): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(source)
  def ofMulti(source: Seq[String]): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(source)
  def ofMulti(source: String, index: Int): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(source, index)
  def ofMulti(source: Seq[String], index: Int): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(source, index)

  override def of(source: String): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(source)
  override def of(source: Seq[String]): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(source)
  override def of(source: String, index: Int): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(source, index)
  override def of(source: Seq[String], index: Int): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(source, index)

  override def ofEach(source: String): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onEach(source)
  override def ofEach(source: Seq[String]): EnrichFunc[EnrichRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onEach(source)

  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = new PipedDependentEnrichFuncWithDefaultField[DependencyRoot, Source, InternalFieldType, ExternalFieldType](this, dependency, field)
  def onMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = new MultiPipedDependentEnrichFuncWithDefaultField[DependencyRoot, Source, InternalFieldType, ExternalFieldType](this, dependency, field)

  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal, field: String)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, field)
  override def onEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, field + "*")

  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal, field: String, index: Int)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, field, index)
  def onMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String, index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, field + s"[$index]")
  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String, index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(dependency, field + s"[$index]")

  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency)
  def onMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = {
    onMulti(dependency, dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField)
  }

  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal, index: Int)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, index)
  def onMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = {
    onMulti(dependency, dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField, index)
  }

  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = {
    on(dependency, dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField)
  }
  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = {
    on(dependency, dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField, index)
  }
  override def onEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = {
    onEach(dependency, dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField)
  }

  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal, field: String)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, field)
  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal, field: String, index: Int)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, field, index)
  def ofMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, field)
  def ofMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String, index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, field, index)
  override def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(dependency, field)
  override def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String, index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(dependency, field, index)
  override def ofEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onEach(dependency, field)

  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency)
  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _] with MultiVal, index: Int)(implicit d: DummyImplicit): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, index)
  def ofMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency)
  def ofMulti[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onMulti(dependency, index)
  override def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(dependency)
  override def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, ExternalFieldType] = on(dependency, index)
  override def ofEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] with DefaultFieldAccess[InternalFieldType, Seq[ExternalFieldType]] with MultiVal = onEach(dependency)
}
