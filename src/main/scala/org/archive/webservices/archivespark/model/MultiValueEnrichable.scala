/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.model

import io.circe.{Json => Circe}
import org.archive.webservices.archivespark.util.Json._
import org.archive.webservices.archivespark.util.{Json, SerializedException}

import scala.collection.immutable.ListMap

class MultiValueEnrichable[T] private (private var _children: Seq[TypedEnrichable[T]]) extends TypedEnrichable[Seq[T]] {
  def children: Seq[TypedEnrichable[T]] = _children
  def get: Seq[T] = children.map(e => e.get)

  override protected[model] def setHierarchy(parent: Enrichable, field: String, root: EnrichRoot): Unit = {
    super.setHierarchy(parent, field, root)
    for ((c, i) <- _children.zipWithIndex) c.setHierarchy(this, s"[$i]", root)
  }

  override protected[archivespark] def excludeFromOutput(value: Boolean, overwrite: Boolean): Unit = {
    super.excludeFromOutput(value, overwrite)
    for (child <- children) child.excludeFromOutput(value, overwrite)
  }

  override def enrich[D](path: Seq[String], func: EnrichFunc[_, D, _], excludeFromOutput: Boolean): Enrichable = {
    if (path.nonEmpty && path.head == "*") {
      var hasEnriched = false
      var lastException: Option[SerializedException] = None
      val enriched = children.map{c =>
        val enriched = c.enrich(path.tail, func, excludeFromOutput)
        if (enriched != c) {
          hasEnriched = true
          lastException = enriched._lastException.orElse(lastException)
          enriched.asInstanceOf[TypedEnrichable[T]]
        } else c
      }
      if (hasEnriched) {
        val clone = copy().asInstanceOf[MultiValueEnrichable[T]]
        clone._children = enriched
        clone._lastException = lastException
        clone
      } else this
    } else if (path.nonEmpty && path.head.matches("\\[\\d+\\]")) {
      val index = path.head.substring(1, path.head.length - 1).toInt
      if (index < children.length) {
        val enriched = children(index).enrich(path.tail, func, excludeFromOutput).asInstanceOf[TypedEnrichable[T]]
        if (children(index) == enriched) this
        else {
          val clone = copy().asInstanceOf[MultiValueEnrichable[T]]
          clone._children = children.zipWithIndex.map{case (c, i) => if (i == index) enriched else c}
          clone._lastException = enriched._lastException
          clone
        }
      } else this
    } else super.enrich(path, func, excludeFromOutput)
  }

  def apply(index: Int): TypedEnrichable[T] = children(index)

  def get(index: Int): T = apply(index).get

  override def apply[D](path: Seq[String]): Option[TypedEnrichable[D]] = {
    if (path.nonEmpty && path.head == "*") {
      val values = children.map(c => c(path.tail)).filter(_.isDefined).map(_.get)
      if (values.isEmpty) None else {
        val clone = new MultiValueEnrichable(values)
        clone.setHierarchy(parent, field, root)
        Some(clone.asInstanceOf[TypedEnrichable[D]])
      }
    } else if (path.nonEmpty && path.head.matches("\\[\\d+\\]")) {
      val index = path.head.substring(1, path.head.length - 1).toInt
      if (index < children.length) children(index).apply(path.tail) else None
    } else super.apply(path)
  }

  def toJson: Map[String, Circe] = {
    val children = _children.map(c => c.toJson).filter(_.nonEmpty).map(mapToJson)
    (if (isExcludedFromOutput && children.isEmpty) Map() else ListMap(
      null.asInstanceOf[String] -> Json.json(children)
    )) ++ enrichments.flatMap { key =>
      val e = enrichment(key).get
      if (e.isTransparent) {
        e.enrichments.map { key =>
          (key, mapToJson(e.enrichment(key).get.toJson))
        }
      } else {
        Iterator((key, mapToJson(e.toJson)))
      }
    }.filter{ case (_, field) => field != null }
  }
}

object MultiValueEnrichable {
  def apply[T](values: Seq[T]): MultiValueEnrichable[T] = new MultiValueEnrichable[T](values.map(v => SingleValueEnrichable(v)))
}