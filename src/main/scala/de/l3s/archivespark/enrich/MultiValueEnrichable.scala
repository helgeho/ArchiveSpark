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

package de.l3s.archivespark.enrich

import de.l3s.archivespark.utils.Json._

import scala.reflect.ClassTag

class MultiValueEnrichable[T] private (val children: Seq[TypedEnrichable[T]], parentEnrichable: Enrichable = null, enrichRoot: EnrichRoot = null) extends TypedEnrichable[Seq[T]] {
  _parent = parentEnrichable
  _root = enrichRoot

  def get: Seq[T] = children.map(e => e.get)

  override protected[archivespark] def excludeFromOutput(value: Boolean, overwrite: Boolean): Unit = {
    for (child <- children) child.excludeFromOutput(value, overwrite)
    super.excludeFromOutput(value, overwrite)
  }

  override def enrich[D](path: Seq[String], func: EnrichFunc[_, D], excludeFromOutput: Boolean): Enrichable = {
    if (path.nonEmpty && path.head == "*") {
      var hasEnriched = false
      val enriched = children.map{c =>
        val enriched = c.enrich(path.tail, func, excludeFromOutput)
        hasEnriched = hasEnriched || enriched != c
        enriched.asInstanceOf[TypedEnrichable[T]]
      }
      if (hasEnriched) {
        val clone = new MultiValueEnrichable(enriched)
        clone._root = _root
        clone._parent = _parent
        clone
      } else this
    } else if (path.nonEmpty && path.head.matches("\\[\\d+\\]")) {
      val index = path.head.substring(1, path.head.length - 1).toInt
      if (index < children.length) {
        val enriched = children.zipWithIndex.map{case (c, i) => if (i == index) c.enrich(path.tail, func, excludeFromOutput).asInstanceOf[TypedEnrichable[T]] else c}
        if (children(index) == enriched(index)) this
        else new MultiValueEnrichable(enriched)
      } else this
    } else super.enrich(path, func, excludeFromOutput)
  }

  def apply(index: Int): TypedEnrichable[T] = children(index)

  def get(index: Int): T = apply(index).get

  override def apply[D : ClassTag](path: Seq[String]): Option[TypedEnrichable[D]] = {
    if (path.nonEmpty && path.head == "*") {
      val values = children.map(c => c(path.tail)).filter(_.isDefined).map(_.get)
      if (values.isEmpty) None else {
        val clone = new MultiValueEnrichable(values)
        clone._root = _root
        clone._parent = _parent
        Some(clone.asInstanceOf[TypedEnrichable[D]])
      }
    } else if (path.nonEmpty && path.head.matches("\\[\\d+\\]")) {
      val index = path.head.substring(1, path.head.length - 1).toInt
      if (index < children.length) children(index)(path.tail) else None
    } else super.apply(path)
  }

  def toJson: Map[String, Any] = (if (isExcludedFromOutput) Map() else Map(
    null.asInstanceOf[String] -> children.map(c => mapToJsonValue(c.toJson))
  )) ++ enrichments.map{e => (e, mapToJsonValue(enrichment(e).get.toJson)) }.filter{ case (_, field) => field != null }
}

object MultiValueEnrichable {
  def apply[T](values: Seq[T], parent: Enrichable = null, root: EnrichRoot = null): MultiValueEnrichable[T] = new MultiValueEnrichable[T](values.map(v => SingleValueEnrichable(v)), parent, root)
}