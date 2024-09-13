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

import org.archive.webservices.archivespark.ArchiveSpark
import org.archive.webservices.archivespark.util.{Copyable, JsonConvertible, SelectorUtil, SerializedException}

import scala.collection.immutable.ListMap
import scala.reflect.ClassTag
import scala.util.Try

trait TypedEnrichable[+T] extends Enrichable {
  def get: T
}

trait Enrichable extends Serializable with Copyable[Enrichable] with JsonConvertible { this: TypedEnrichable[_] =>
  def get: Any
  def typed[T]: TypedEnrichable[T] = this.asInstanceOf[TypedEnrichable[T]]

  var isTransparent: Boolean = true

  private var excludeFromOutput: Option[Boolean] = None
  def isExcludedFromOutput: Boolean = excludeFromOutput match {
    case Some(value) => value
    case None => false
  }

  private[model] var _lastException: Option[SerializedException] = None
  def lastException: Option[SerializedException] = _lastException

  private[archivespark] def excludeFromOutput(value: Boolean = true, overwrite: Boolean = true): Unit = excludeFromOutput match {
    case Some(_) => if (overwrite) excludeFromOutput = Some(value)
    case None => excludeFromOutput = Some(value)
  }

  @transient private var _field: String = _
  def field: String = _field

  @transient private var _parent: Enrichable = _
  def parent: Enrichable = _parent
  def parentAs[A]: TypedEnrichable[A] = _parent.asInstanceOf[TypedEnrichable[A]]

  @transient private var _root: EnrichRoot = _
  def root[A]: TypedEnrichRoot[A] = _root.asInstanceOf[TypedEnrichRoot[A]]

  def path: Seq[String] = if (_parent == null) Seq.empty else _parent.path :+ _field
  def chain: Seq[Enrichable] = if (_parent == null) Seq(this) else _parent.chain :+ this

  protected[model] def setHierarchy(parent: Enrichable, field: String, root: EnrichRoot): Unit = {
    _field = field
    _parent = parent
    _root = root
  }

  private var _enrichments: Map[String, Enrichable] = ListMap[String, Enrichable]()
  def enrichments: Set[String] = _enrichments.keySet

  def enrichment[D : ClassTag](field: String): Option[TypedEnrichable[D]] = {
    _enrichments.get(field) match {
      case Some(enrichable) =>
        enrichable.setHierarchy(this, field, root)
        Some(enrichable.asInstanceOf[TypedEnrichable[D]])
      case None => None
    }
  }

  def enrich(fieldName: String, enrichment: Enrichable): Enrichable = {
    val clone = copy()
    clone._enrichments = clone._enrichments.updated(fieldName, enrichment)
    clone._lastException = enrichment._lastException
    clone
  }

  def enrichValue[Value](fieldName: String, value: Value): Enrichable = {
    enrich(fieldName, SingleValueEnrichable[Value](value))
  }

  private def enrich[D](func: EnrichFunc[_, D, _], excludeFromOutput: Boolean = false): Enrichable = {
    if (!func.isEnriched(this)) {
      val derivatives = new Derivatives(func.fields, transparent = func.isTransparent)
      var lastException: Option[SerializedException] = None
      try {
        func.derive(this.asInstanceOf[TypedEnrichable[D]], derivatives)
      } catch {
        case e: Exception =>
          if (ArchiveSpark.conf.catchExceptions) {
            e.printStackTrace()
            lastException = Some(SerializedException(e))
          } else throw e
      }
      val clone = copy()
      clone._lastException = lastException
      for ((field, enrichment) <- derivatives.get) {
        enrichment.excludeFromOutput(excludeFromOutput, overwrite = false)
        clone._enrichments = clone._enrichments.updated(field, enrichment)
      }
      clone
    } else if (!excludeFromOutput) {
      val excluded = func.fields.map(enrichment).filter(_.isDefined).map(_.get).filter(_.isExcludedFromOutput)
      if (excluded.nonEmpty) {
        val clone = copy()
        for (enrichment <- excluded) {
          val enrichmentClone = enrichment.copy()
          enrichmentClone.excludeFromOutput(value = false)
          clone._enrichments = clone._enrichments.updated(enrichment.field, enrichmentClone)
        }
        clone
      } else {
        this
      }
    } else {
      this
    }
  }

  private[model] def enrich[D](path: Seq[String], func: EnrichFunc[_, D, _], excludeFromOutput: Boolean): Enrichable = {
    if (path.isEmpty || (path.length == 1 && path.head == "")) enrich(func, excludeFromOutput)
    else {
      val field = path.head
      enrichment(field) match {
        case Some(enrichable) =>
          val enriched = enrichable.enrich(path.tail, func, excludeFromOutput)
          if (enriched == enrichable) this
          else enrich(field, enriched)
        case None => this
      }
    }
  }

  def apply[D](path: Seq[String]): Option[TypedEnrichable[D]] = {
    if (path.isEmpty || (path.length == 1 && path.head == "")) Some(this.asInstanceOf[TypedEnrichable[D]])
    else {
      if (path.head == "") {
        val remaining = path.tail
        enrichment(remaining.head) match {
          case Some(child) => child(remaining.tail)
          case None =>
            for (child <- _enrichments.values) {
              val target: Option[TypedEnrichable[D]] = child[D](path)
              if (target.isDefined) return target
            }
            None
        }
      } else if (path.head.matches("\\[\\d+\\]")) {
        val index = path.head.substring(1, path.head.length - 1).toInt
        if (index > 0) None else apply(path.tail)
      } else if (path.head == "*") {
        apply(path.tail)
      } else {
        enrichment(path.head) match {
          case Some(child) => child(path.tail)
          case None => None
        }
      }
    }
  }

  def apply[D](key: String): Option[TypedEnrichable[D]] = apply(SelectorUtil.parse(key))

  def get[D : ClassTag](path: String): Option[D] = get(SelectorUtil.parse(path))
  def get[D : ClassTag](path: Seq[String]): Option[D] = Try {
    apply[D](path) match {
      case Some(enrichable) => Some(enrichable.get)
      case None => None
    }
  }.toOption.flatten
}
