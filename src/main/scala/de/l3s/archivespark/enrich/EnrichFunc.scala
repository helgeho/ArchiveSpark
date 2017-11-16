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

import scala.util.Try

trait EnrichFunc[Root <: EnrichRoot, Source] extends Serializable {
  def source: Seq[String]
  def fields: Seq[String]

  def pathTo(field: String): Seq[String] = source ++ SelectorUtil.parse(field)

  def enrich(root: Root): Root = enrich(root, excludeFromOutput = false)

  private[enrich] def enrich(root: Root, excludeFromOutput: Boolean): Root = root.enrich(source, this, excludeFromOutput).asInstanceOf[Root]

  protected[enrich] def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit

  def aliases: Map[String, String] = Map.empty

  def isEnriched(root: Root): Boolean = root(source) match {
    case Some(source) => exists(source)
    case None => false
  }

  def exists(source: Enrichable): Boolean = fields.forall(f => source.enrichment(f).isDefined)

  def prepareGlobal(rdd: RDD[Root]): RDD[Any] = rdd.asInstanceOf[RDD[Any]]
  def prepareLocal(record: Any): Root = record.asInstanceOf[Root]

  def hasField(name: String): Boolean = (aliases.keySet ++ fields).contains(name)

  def onRoot: EnrichFunc[EnrichRoot, Source] = on(Seq.empty)
  def ofRoot: EnrichFunc[EnrichRoot, Source] = onRoot

  def on(source: Seq[String]): EnrichFunc[EnrichRoot, Source] = new PipedEnrichFunc[Source](this, source)
  def on(source: String): EnrichFunc[EnrichRoot, Source] = on(SelectorUtil.parse(source))
  def on(source: String, index: Int): EnrichFunc[EnrichRoot, Source] = on(SelectorUtil.parse(source), index)
  def on(source: Seq[String], index: Int): EnrichFunc[EnrichRoot, Source] = on(source :+ s"[$index]")
  def onEach(source: String): EnrichFunc[EnrichRoot, Source] = onEach(SelectorUtil.parse(source))
  def onEach(source: Seq[String]): EnrichFunc[EnrichRoot, Source] = on(source :+ "*")

  def of(source: String): EnrichFunc[EnrichRoot, Source] = on(source)
  def of(source: Seq[String]): EnrichFunc[EnrichRoot, Source] = on(source)
  def of(source: String, index: Int): EnrichFunc[EnrichRoot, Source] = on(source, index)
  def of(source: Seq[String], index: Int): EnrichFunc[EnrichRoot, Source] = on(source, index)
  def ofEach(source: String): EnrichFunc[EnrichRoot, Source] = onEach(source)
  def ofEach(source: Seq[String]): EnrichFunc[EnrichRoot, Source] = onEach(source)

  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] = new PipedDependentEnrichFunc[DependencyRoot, Source](this, dependency, field)
  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String, index: Int): EnrichFunc[DependencyRoot, Source] = on(dependency, field + s"[$index]")
  def onEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] = on(dependency, field + "*")

  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = on(dependency, dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField)
  def on[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] = on(dependency, dependency.asInstanceOf[DefaultFieldAccess[Source, _]].defaultField, index)
  def onEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = onEach(dependency, dependency.asInstanceOf[DefaultFieldAccess[Seq[Source], _]].defaultField)

  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] = on(dependency, field)
  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String, index: Int): EnrichFunc[DependencyRoot, Source] = on(dependency, field, index)
  def ofEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], field: String): EnrichFunc[DependencyRoot, Source] = onEach(dependency, field)

  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = on(dependency)
  def of[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _], index: Int): EnrichFunc[DependencyRoot, Source] = on(dependency, index)
  def ofEach[DependencyRoot <: EnrichRoot](dependency: EnrichFunc[DependencyRoot, _]): EnrichFunc[DependencyRoot, Source] = onEach(dependency)

  def map[SourceField, Target](target: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = map[SourceField, Target](Try {this.asInstanceOf[DefaultFieldAccess[SourceField, _]].defaultField}.getOrElse(fields.head), target, target)(f)
  def map[SourceField, Target](sourceField: String, target: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = map[SourceField, Target](sourceField, target, target)(f)
  def map[SourceField, Target](sourceField: String, target: String, alias: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = {
    val dependencyFunction = this
    new DependentEnrichFuncWithDefaultField[Root, SourceField, Target, Target] with SingleField[Target] {
      override def dependency: EnrichFunc[Root, Source] = dependencyFunction
      override def dependencyField: String = sourceField
      override def resultField: String = target
      override def aliases = Map(alias -> target)
      override def derive(source: TypedEnrichable[SourceField], derivatives: Derivatives): Unit = derivatives << f(source.get)
    }
  }

  def mapEach[SourceField, Target](target: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with DefaultFieldAccess[Target, Seq[Target]] = mapEach[SourceField, Target](Try {this.asInstanceOf[DefaultFieldAccess[Seq[SourceField], _]].defaultField}.getOrElse(fields.head), target, target)(f)
  def mapEach[SourceField, Target](sourceField: String, target: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with DefaultFieldAccess[Target, Seq[Target]] = mapEach[SourceField, Target](sourceField, target, target)(f)
  def mapEach[SourceField, Target](sourceField: String, target: String, alias: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with DefaultFieldAccess[Target, Seq[Target]] = {
    val dependencyFunction = this
    new DependentEnrichFuncWithDefaultField[Root, SourceField, Target, Seq[Target]] with DefaultFieldAccess[Target, Seq[Target]] with MultiVal {
      override def dependency: EnrichFunc[Root, _] = dependencyFunction
      override def dependencyField: String = sourceField + "*"
      override def fields: Seq[String] = Seq(target)
      override def defaultField: String = target
      override def aliases = Map(alias -> target)
      override def derive(source: TypedEnrichable[SourceField], derivatives: Derivatives): Unit = derivatives << f(source.get)
    }
  }

  def mapMulti[SourceField, Target](target: String)(f: SourceField => Iterable[Target]): DependentEnrichFunc[Root, SourceField] with SingleField[Seq[Target]] = mapMulti[SourceField, Target](Try {this.asInstanceOf[DefaultFieldAccess[Seq[SourceField], _]].defaultField}.getOrElse(fields.head), target, target)(f)
  def mapMulti[SourceField, Target](sourceField: String, target: String)(f: SourceField => Iterable[Target]): DependentEnrichFunc[Root, SourceField] with SingleField[Seq[Target]] = mapMulti[SourceField, Target](sourceField, target, target)(f)
  def mapMulti[SourceField, Target](sourceField: String, target: String, alias: String)(f: SourceField => Iterable[Target]): DependentEnrichFunc[Root, SourceField] with SingleField[Seq[Target]] = {
    val dependencyFunction = this
    new DependentEnrichFuncWithDefaultField[Root, SourceField, Seq[Target], Seq[Target]] with SingleField[Seq[Target]] {
      override def dependency: EnrichFunc[Root, _] = dependencyFunction
      override def dependencyField: String = sourceField
      override def resultField: String = target
      override def aliases = Map(alias -> target)
      override def derive(source: TypedEnrichable[SourceField], derivatives: Derivatives): Unit = {
        val mapped = f(source.get)
        derivatives.setNext(MultiValueEnrichable(mapped.toSeq))
      }
    }
  }
}