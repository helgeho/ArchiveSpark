/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.nativescala.implicits.classes

import de.l3s.archivespark.enrich._
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.nativescala.implicits._
import de.l3s.archivespark.utils.SelectorUtil

import scala.reflect.ClassTag

class EnrichableTraversable[Root <: EnrichRoot : ClassTag](records: Traversable[Root]) {
  def enrich[SpecificRoot >: Root <: EnrichRoot](f: EnrichFunc[SpecificRoot, _]): Traversable[Root] = records.map(r => f.enrich(r).asInstanceOf[Root])

  def mapEnrich[Source, Target](sourceField: String, target: String)(f: Source => Target): Traversable[Root] = mapEnrich(SelectorUtil.parse(sourceField), target, target)(f)
  def mapEnrich[Source, Target](sourceField: String, target: String, alias: String)(f: Source => Target): Traversable[Root] = mapEnrich(SelectorUtil.parse(sourceField), target, alias)(f)
  def mapEnrich[Source, Target](sourceField: Seq[String], target: String)(f: Source => Target): Traversable[Root] = mapEnrich(sourceField, target, target)(f)
  def mapEnrich[Source, Target](sourceField: Seq[String], target: String, alias: String)(f: Source => Target): Traversable[Root] = {
    val enrichFunc = new EnrichFunc[Root, Source] {
      override def source: Seq[String] = sourceField
      override def fields: Seq[String] = Seq(target)
      override def aliases = Map(alias -> target)
      override def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit = derivatives << f(source.get)
    }
    records.map(r => enrichFunc.enrich(r))
  }

  def mapEnrich[SpecificRoot >: Root <: EnrichRoot, Source, Target](dependencyFunc: EnrichFunc[SpecificRoot, _] with DefaultField[Source], target: String)(f: Source => Target): Traversable[Root] = mapEnrich(dependencyFunc, dependencyFunc.defaultField, target, target)(f)
  def mapEnrich[SpecificRoot >: Root <: EnrichRoot, Source, Target](dependencyFunc: EnrichFunc[SpecificRoot, _], sourceField: String, target: String)(f: Source => Target): Traversable[Root] = mapEnrich(dependencyFunc, sourceField, target, target)(f)
  def mapEnrich[SpecificRoot >: Root <: EnrichRoot, Source, Target](dependencyFunc: EnrichFunc[SpecificRoot, _], sourceField: String, target: String, alias: String)(f: Source => Target): Traversable[Root] = {
    val enrichFunc = new DependentEnrichFunc[SpecificRoot, Source] {
      override def dependency: EnrichFunc[SpecificRoot, _] = dependencyFunc
      override def dependencyField: String = sourceField
      override def fields: Seq[String] = Seq(target)
      override def aliases = Map(alias -> target)
      override def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit = derivatives << f(source.get)
    }
    records.map(r => enrichFunc.enrich(r).asInstanceOf[Root])
  }

  def filterExists(path: String): Traversable[Root] = records.filter(r => r[Nothing](path).isDefined)
  def filterExists[SpecificRoot >: Root <: EnrichRoot](f: EnrichFunc[SpecificRoot, _]): Traversable[Root] = records.filter(r => f.isEnriched(r))

  def filterValue[Source : ClassTag](field: Seq[String])(filter: Option[Source] => Boolean): Traversable[Root] = records.filter(r => filter(r.get[Source](field)))
  def filterValue[Source : ClassTag](field: String)(filter: Option[Source] => Boolean): Traversable[Root] = filterValue(SelectorUtil.parse(field))(filter)
  def filterValue[SpecificRoot >: Root <: EnrichRoot, Source : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String)(filter: Option[Source] => Boolean): Traversable[Root] = {
    filterValue(f.pathTo(field))(filter)
  }
  def filterValue[SpecificRoot >: Root <: EnrichRoot, Source : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[Source])(filter: Option[Source] => Boolean): Traversable[Root] = {
    filterValue(f.pathToDefaultField)(filter)
  }

  def filterNonEmpty(field: Seq[String]): Traversable[Root] = {
    records.filter{r =>
      r.get[{def nonEmpty: Boolean}](field) match {
        case Some(value) => value.nonEmpty
        case _ => false
      }
    }
  }
  def filterNonEmpty(field: String): Traversable[Root] = filterNonEmpty(SelectorUtil.parse(field))
  def filterNonEmpty[SpecificRoot >: Root <: EnrichRoot](f: EnrichFunc[SpecificRoot, _], field: String): Traversable[Root] = filterNonEmpty(f.pathTo(field))
  def filterNonEmpty[SpecificRoot >: Root <: EnrichRoot](f: EnrichFunc[SpecificRoot, _]): Traversable[Root] = {
    records.filter{r =>
      val parent = r(f.source)
      parent.isDefined && parent.get.enrichments.map(key => parent.get.enrichment[{def nonEmpty: Boolean}](key).map(_.get)).exists{
        case Some(value) => value.nonEmpty
        case _ => false
      }
    }
  }

  def distinctValue[T : ClassTag](value: Root => T)(distinct: (Root, Root) => Root): Traversable[Root] = {
    records.groupBy(r => value(r)).mapValues(_.reduce(distinct)).values
  }
  def distinctValue(field: Seq[String])(distinct: (Root, Root) => Root): Traversable[Root] = {
    records.groupBy(r => r.get(field)).mapValues(_.reduce(distinct)).values
  }
  def distinctValue(field: String)(distinct: (Root, Root) => Root): Traversable[Root] = {
    distinctValue(SelectorUtil.parse(field))(distinct)
  }
  def distinctValue[SpecificRoot >: Root <: EnrichRoot](f: EnrichFunc[SpecificRoot, _], field: String)(distinct: (Root, Root) => Root): Traversable[Root] = {
    distinctValue(f.pathTo(field))(distinct)
  }
  def distinctValue[SpecificRoot >: Root <: EnrichRoot, Source : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[Source])(distinct: (Root, Root) => Root): Traversable[Root] = {
    distinctValue(f.pathToDefaultField)(distinct)
  }

  def mapValues[T : ClassTag](path: String): Traversable[T] = records.map(r => r.get[T](path)).filter(o => o.isDefined).map(o => o.get)
  def mapValues[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[T]): Traversable[T] = records.enrich(f).map(_.value[SpecificRoot, T](f)).filter(_.isDefined).map(_.get)
  def mapValues[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String): Traversable[T] = records.enrich(f).map(_.value[SpecificRoot, T](f, field)).filter(_.isDefined).map(_.get)
  def mapMultiValues[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[T]): Traversable[Seq[T]] = records.enrich(f).map(_.values[SpecificRoot, T](f)).filter(_.isDefined).map(_.get)
  def mapMultiValues[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String): Traversable[Seq[T]] = records.enrich(f).map(_.values[SpecificRoot, T](f, field)).filter(_.isDefined).map(_.get)
}
