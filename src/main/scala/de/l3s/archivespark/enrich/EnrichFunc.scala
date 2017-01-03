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

  def pathTo(field: String) = source ++ SelectorUtil.parse(field)

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
  def prepareLocal(record: Any) = record.asInstanceOf[Root]

  def hasField(name: String) = (aliases.keySet ++ fields).contains(name)

  def map[SourceField, Target](target: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = map[SourceField, Target](Try {this.asInstanceOf[DefaultField[SourceField]].defaultField}.getOrElse(fields.head), target, target)(f)
  def map[SourceField, Target](sourceField: String, target: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = map[SourceField, Target](sourceField, target, target)(f)
  def map[SourceField, Target](sourceField: String, target: String, alias: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = {
    val dependencyFunction = this
    new DependentEnrichFunc[Root, SourceField] with SingleField[Target] {
      override def dependency = dependencyFunction
      override def dependencyField = sourceField
      override def resultField = target
      override def aliases = Map(alias -> target)
      override def derive(source: TypedEnrichable[SourceField], derivatives: Derivatives): Unit = derivatives << f(source.get)
    }
  }

  def mapEach[SourceField, Target](target: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = mapEach[SourceField, Target](Try {this.asInstanceOf[DefaultField[Seq[SourceField]]].defaultField}.getOrElse(fields.head), target, target)(f)
  def mapEach[SourceField, Target](sourceField: String, target: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = mapEach[SourceField, Target](sourceField, target, target)(f)
  def mapEach[SourceField, Target](sourceField: String, target: String, alias: String)(f: SourceField => Target): DependentEnrichFunc[Root, SourceField] with SingleField[Target] = map(sourceField + "*", target, alias)(f)

  def flatMap[SourceField, Target](target: String)(f: Iterable[SourceField] => Iterable[SourceField]): DependentEnrichFunc[Root, Seq[SourceField]] with SingleField[Seq[Target]] = flatMap[SourceField, Target](Try {this.asInstanceOf[DefaultField[SourceField]].defaultField}.getOrElse(fields.head), target, target)(f)
  def flatMap[SourceField, Target](sourceField: String, target: String)(f: Iterable[SourceField] => Iterable[SourceField]): DependentEnrichFunc[Root, Seq[SourceField]] with SingleField[Seq[Target]] = flatMap[SourceField, Target](sourceField, target, target)(f)
  def flatMap[SourceField, Target](sourceField: String, target: String, alias: String)(f: Iterable[SourceField] => Iterable[SourceField]): DependentEnrichFunc[Root, Seq[SourceField]] with SingleField[Seq[Target]] = {
    val dependencyFunction = this
    new DependentEnrichFunc[Root, Seq[SourceField]] with SingleField[Target] {
      override def dependency = dependencyFunction
      override def dependencyField = sourceField
      override def resultField = target
      override def aliases = Map(alias -> target)
      override def derive(source: TypedEnrichable[Seq[SourceField]], derivatives: Derivatives): Unit = {
        val mapped = f(source.get)
        derivatives.setNext(MultiValueEnrichable[SourceField](mapped.toSeq))
      }
    }
  }
}