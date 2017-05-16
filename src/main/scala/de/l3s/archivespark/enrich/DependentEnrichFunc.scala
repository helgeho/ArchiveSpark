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
import org.apache.spark.rdd.RDD

trait DependentEnrichFunc[Root <: EnrichRoot, Source] extends EnrichFunc[Root, Source] {
  def dependency: EnrichFunc[Root, _]

  def dependencyField: String

  override def source: Seq[String] = dependency.pathTo(dependencyField)

  override def prepareGlobal(rdd: RDD[Root]): RDD[Any] = dependency.prepareGlobal(rdd)
  override def prepareLocal(record: Any): Root = dependency.prepareLocal(record)

  override protected[enrich] def enrich(root: Root, excludeFromOutput: Boolean): Root = super.enrich(dependency.enrich(root, excludeFromOutput = true), excludeFromOutput)

  def onRoot: EnrichFunc[_, Source] = new PipedEnrichFunc[Source](this, Seq())
  def ofRoot = onRoot

  def on(source: Seq[String]): EnrichFunc[_, Source] = new PipedEnrichFunc[Source](this, source)
  def on(source: String): EnrichFunc[_, Source] = on(SelectorUtil.parse(source))
  def on(source: String, index: Int): EnrichFunc[_, Source] = on(SelectorUtil.parse(source), index)
  def on(source: Seq[String], index: Int): EnrichFunc[_, Source] = on(source :+ s"[$index]")
  def onEach(source: String): EnrichFunc[_, Source] = onEach(SelectorUtil.parse(source))
  def onEach(source: Seq[String]): EnrichFunc[_, Source] = on(source :+ "*")

  def of(source: String): EnrichFunc[_, Source] = on(source)
  def of(source: Seq[String]): EnrichFunc[_, Source] = on(source)
  def of(source: String, index: Int): EnrichFunc[_, Source] = on(source, index)
  def of(source: Seq[String], index: Int): EnrichFunc[_, Source] = on(source, index)
  def ofEach(source: String): EnrichFunc[_, Source] = onEach(source)
  def ofEach(source: Seq[String]): EnrichFunc[_, Source] = onEach(source)

  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] = new PipedDependentEnrichFunc[DependencyRoot, Source](this, dependency, field)
  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String, index: Int): EnrichFunc[DependencyRoot, Source] = on(dependency, field + s"[$index]")
  def onEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] = on(dependency, field + "*")

  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = {
    on(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source]].defaultField)
  }
  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] = {
    on(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source]].defaultField, index)
  }
  def onEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = {
    onEach(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source]].defaultField)
  }

  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] = on(dependency, field)
  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String, index: Int): EnrichFunc[DependencyRoot, Source] = on(dependency, field, index)
  def ofEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] = onEach(dependency, field)

  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = on(dependency)
  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] = on(dependency, index)
  def ofEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = onEach(dependency)
}
