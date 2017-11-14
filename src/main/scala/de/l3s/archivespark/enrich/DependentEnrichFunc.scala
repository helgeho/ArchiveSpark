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

import org.apache.spark.rdd.RDD

trait DependentEnrichFunc[Root <: EnrichRoot, Source] extends EnrichFunc[Root, Source] {
  def dependency: EnrichFunc[Root, _]

  def dependencyField: String

  override def source: Seq[String] = dependency.pathTo(dependencyField)

  override def prepareGlobal(rdd: RDD[Root]): RDD[Any] = dependency.prepareGlobal(rdd)
  override def prepareLocal(record: Any): Root = dependency.prepareLocal(record)

  override protected[enrich] def enrich(root: Root, excludeFromOutput: Boolean): Root = super.enrich(dependency.enrich(root, excludeFromOutput = true), excludeFromOutput)

  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = {
    on(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField)
  }
  override def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] = {
    on(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField, index)
  }
  override def onEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = {
    onEach(dependency, if (dependency.hasField(dependencyField)) dependencyField else dependency.asInstanceOf[DefaultFieldAccess[Seq[Source], _]].defaultField)
  }
}
