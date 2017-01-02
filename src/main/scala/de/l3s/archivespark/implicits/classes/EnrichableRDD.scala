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

package de.l3s.archivespark.implicits.classes

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.enrich._
import de.l3s.archivespark.enrich.grouping.{GroupContext, GroupRecord}
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.utils.SelectorUtil
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Try

class EnrichableRDD[Root <: EnrichRoot : ClassTag](rdd: RDD[Root]) {
  def enrich[SpecificRoot >: Root <: EnrichRoot : ClassTag](func: EnrichFunc[SpecificRoot, _]): RDD[Root] = {
    func.prepareGlobal(rdd.map(_.asInstanceOf[SpecificRoot])).mapPartitions { records =>
      records.map(r =>
        func.enrich(func.prepareLocal(r)).asInstanceOf[Root]
      )
    }
  }

  def group(fields: (String, Root => Any)*) = {
    val keyRecordPairs = rdd.mapPartitions { records =>
      records.map{r =>
        val meta = fields.map{case (k, f) => (k, f(r))}.toMap
        (meta, r)
      }
    }
    val records = keyRecordPairs.map{case (k, v) => k}.distinct(ArchiveSpark.partitions(rdd.context)).map(new GroupRecord(_)).cache
    (records, new GroupContext[Root](keyRecordPairs))
  }

  def mapEnrich[Source, Target](sourceField: String, target: String)(f: Source => Target): RDD[Root] = mapEnrich(SelectorUtil.parse(sourceField), target, target)(f)
  def mapEnrich[Source, Target](sourceField: String, target: String, alias: String)(f: Source => Target): RDD[Root] = mapEnrich(SelectorUtil.parse(sourceField), target, alias)(f)
  def mapEnrich[Source, Target](sourceField: Seq[String], target: String)(f: Source => Target): RDD[Root] = mapEnrich(sourceField, target, target)(f)
  def mapEnrich[Source, Target](sourceField: Seq[String], target: String, alias: String)(f: Source => Target): RDD[Root] = rdd.enrich(new EnrichFunc[Root, Source] {
    override def source: Seq[String] = sourceField
    override def fields: Seq[String] = Seq(target)
    override def aliases = Map(alias -> target)
    override def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit = derivatives << f(source.get)
  })

  def mapEnrich[SpecificRoot >: Root <: EnrichRoot : ClassTag, Source, Target](dependencyFunc: EnrichFunc[SpecificRoot, _] with DefaultField[Source], target: String)(f: Source => Target): RDD[Root] = mapEnrich(dependencyFunc, dependencyFunc.defaultField, target, target)(f)
  def mapEnrich[SpecificRoot >: Root <: EnrichRoot : ClassTag, Source, Target](dependencyFunc: EnrichFunc[SpecificRoot, _], sourceField: String, target: String)(f: Source => Target): RDD[Root] = mapEnrich(dependencyFunc, sourceField, target, target)(f)
  def mapEnrich[SpecificRoot >: Root <: EnrichRoot : ClassTag, Source, Target](dependencyFunc: EnrichFunc[SpecificRoot, _], sourceField: String, target: String, alias: String)(f: Source => Target): RDD[Root] = rdd.enrich(dependencyFunc.map(sourceField, target, alias)(f))

  def filterExists(path: String): RDD[Root] = rdd.filter(r => r(path).isDefined)
  def filterExists[SpecificRoot >: Root <: EnrichRoot](f: EnrichFunc[SpecificRoot, _]): RDD[Root] = rdd.filter(r => f.isEnriched(r))

  def filterNoException(): RDD[Root] = rdd.filter(r => r.lastException.isDefined)
  def lastException: Option[Exception] = Try{rdd.filter(r => r.lastException.isDefined).take(1).head.lastException.get}.toOption
  def throwLastException(): Unit = lastException match {
    case Some(e) => throw e
    case _ =>
  }

  def filterValue[Source : ClassTag](field: Seq[String])(filter: Option[Source] => Boolean): RDD[Root] = rdd.filter(r => filter(r.get[Source](field)))
  def filterValue[Source : ClassTag](field: String)(filter: Option[Source] => Boolean): RDD[Root] = filterValue(SelectorUtil.parse(field))(filter)
  def filterValue[SpecificRoot >: Root <: EnrichRoot, Source : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String)(filter: Option[Source] => Boolean): RDD[Root] = {
    filterValue(f.pathTo(field))(filter)
  }
  def filterValue[SpecificRoot >: Root <: EnrichRoot, Source : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[Source])(filter: Option[Source] => Boolean): RDD[Root] = {
    filterValue(f.pathToDefaultField)(filter)
  }

  def filterNonEmpty(field: Seq[String]): RDD[Root] = {
    type NonEmpty = {def nonEmpty: Boolean}
    rdd.filter{r =>
      r.get[NonEmpty](field) match {
        case Some(value) => value.nonEmpty
        case _ => false
      }
    }
  }
  def filterNonEmpty(field: String): RDD[Root] = filterNonEmpty(SelectorUtil.parse(field))
  def filterNonEmpty[SpecificRoot >: Root <: EnrichRoot](f: EnrichFunc[SpecificRoot, _], field: String): RDD[Root] = filterNonEmpty(f.pathTo(field))
  def filterNonEmpty[SpecificRoot >: Root <: EnrichRoot, Source : ClassTag](f: EnrichFunc[SpecificRoot, Source]): RDD[Root] = {
    type NonEmpty = {def nonEmpty: Boolean}
    rdd.filter{r =>
      val parent = r[Source](f.source)
      parent.isDefined && parent.get.enrichments.map(key => parent.get.enrichment[NonEmpty](key).map(_.get)).exists {
        case Some(value) => value.nonEmpty
        case _ => false
      }
    }
  }

  def distinctValue[T : ClassTag](value: Root => T)(distinct: (Root, Root) => Root): RDD[Root] = {
    rdd.map(r => (value(r), r)).reduceByKey(distinct, ArchiveSpark.partitions(rdd.sparkContext)).values
  }
  def distinctValue(field: Seq[String])(distinct: (Root, Root) => Root): RDD[Root] = {
    rdd.map(r => (r.get(field), r)).reduceByKey(distinct, ArchiveSpark.partitions(rdd.sparkContext)).values
  }
  def distinctValue(field: String)(distinct: (Root, Root) => Root): RDD[Root] = {
    distinctValue(SelectorUtil.parse(field))(distinct)
  }
  def distinctValue[SpecificRoot >: Root <: EnrichRoot](f: EnrichFunc[SpecificRoot, _], field: String)(distinct: (Root, Root) => Root): RDD[Root] = {
    distinctValue(f.pathTo(field))(distinct)
  }
  def distinctValue[SpecificRoot >: Root <: EnrichRoot, Source : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[Source])(distinct: (Root, Root) => Root): RDD[Root] = {
    distinctValue(f.pathToDefaultField)(distinct)
  }

  def mapValues[T : ClassTag](path: String): RDD[T] = rdd.map(r => r.get[T](path)).filter(_.isDefined).map(_.get)
  def mapValues[SpecificRoot >: Root <: EnrichRoot : ClassTag, T : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[T]): RDD[T] = rdd.enrich(f).flatMap(_.value[SpecificRoot, T](f))
  def mapValues[SpecificRoot >: Root <: EnrichRoot : ClassTag, T : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String): RDD[T] = rdd.enrich(f).flatMap(_.value[SpecificRoot, T](f, field))
  def mapMultiValues[SpecificRoot >: Root <: EnrichRoot : ClassTag, T : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[T]): RDD[Seq[T]] = rdd.enrich(f).flatMap(_.values[SpecificRoot, T](f))
  def mapMultiValues[SpecificRoot >: Root <: EnrichRoot : ClassTag, T : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String): RDD[Seq[T]] = rdd.enrich(f).flatMap(_.values[SpecificRoot, T](f, field))
  def flatMultiValues[SpecificRoot >: Root <: EnrichRoot : ClassTag, T : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultField[T]): RDD[T] = mapMultiValues[SpecificRoot, T](f).flatMap(r => r)
  def flatMultiValues[SpecificRoot >: Root <: EnrichRoot : ClassTag, T : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String): RDD[T] = mapMultiValues[SpecificRoot, T](f, field).flatMap(r => r)
}
