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

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.utils.{Copyable, JsonConvertible, SelectorUtil}

import scala.reflect.ClassTag

trait TypedEnrichable[+T] extends Enrichable {
  def get: T
}

trait Enrichable extends Serializable with Copyable[Enrichable] with JsonConvertible { this: TypedEnrichable[_] =>
  def get: Any
  def typed[T]: TypedEnrichable[T] = this.asInstanceOf[TypedEnrichable[T]]

  override def copy(): Enrichable = copy(Map.empty)

  protected[enrich] def copy(cloned: Map[String, Enrichable]): Enrichable = {
    val copy = super.copy()
    copy._enrichments = (_enrichments.keySet ++ cloned.keySet).map{field =>
      val enrichable = cloned.getOrElse(field, _enrichments(field).copy())
      enrichable.setHierarchy(copy, field, _root)
      (field, enrichable)
    }.toMap
    copy
  }

  private var excludeFromOutput: Option[Boolean] = None
  def isExcludedFromOutput: Boolean = excludeFromOutput match {
    case Some(value) => value
    case None => false
  }

  private[enrich] var _lastException: Option[Exception] = None
  def lastException: Option[Exception] = _lastException

  private[archivespark] def excludeFromOutput(value: Boolean = true, overwrite: Boolean = true): Unit = excludeFromOutput match {
    case Some(_) => if (overwrite) excludeFromOutput = Some(value)
    case None => excludeFromOutput = Some(value)
  }

  private var _field: String = _
  def field: String = _field

  private var _parent: Enrichable = _
  def parent[A]: TypedEnrichable[A] = _parent.asInstanceOf[TypedEnrichable[A]]

  private var _root: EnrichRoot = _
  def root[A]: TypedEnrichRoot[A] = _root.asInstanceOf[TypedEnrichRoot[A]]

  def path: Seq[String] = if (_parent == null) Seq.empty else _parent.path :+ _field
  def chain: Seq[Enrichable] = if (_parent == null) Seq(this) else _parent.chain :+ this

  protected[enrich] def setHierarchy(parent: Enrichable, field: String, root: EnrichRoot = null): Unit = {
    _field = field
    _parent = parent
    _root = root
  }

  private var _enrichments = Map[String, Enrichable]()
  def enrichments: Set[String] = _enrichments.keySet

  private var _aliases = Map[String, String]()
  def field(key: String): String = _aliases.getOrElse(key, key)
  def setAlias(fieldName: String, alias: String): Enrichable = {
    val clone = copy()
    clone._aliases += alias -> fieldName
    clone
  }

  def enrichment[D : ClassTag](key: String): Option[TypedEnrichable[D]] = _enrichments.get(field(key)).map(_.asInstanceOf[TypedEnrichable[D]])

  def enrich(fieldName: String, enrichment: Enrichable): Enrichable = {
    val clone = copy(Map(fieldName -> enrichment))
    clone._lastException = enrichment._lastException
    clone._aliases -= fieldName
    clone
  }

  def enrichValue[Value](fieldName: String, value: Value): Enrichable = {
    val enrichable = SingleValueEnrichable[Value](value, this, fieldName, _root)
    enrich(fieldName, enrichable)
  }

  private[enrich] def enrich[D](func: EnrichFunc[_, D], excludeFromOutput: Boolean = false): Enrichable = {
    if (!func.exists(this)) {
      val derivatives = new Derivatives(func.fields, func.aliases)
      var lastException: Option[Exception] = None
      try {
        func.derive(this.asInstanceOf[TypedEnrichable[D]], derivatives)
      } catch {
        case exception: Exception =>
          if (ArchiveSpark.catchExceptions) lastException = Some(exception)
          else throw exception
      }
      val clone = copy(derivatives.get)
      clone._lastException = lastException
      clone._aliases ++= derivatives.aliases
      for ((field, enrichment) <- derivatives.get) {
        enrichment.excludeFromOutput(excludeFromOutput, overwrite = false)
        clone._aliases -= field
      }
      clone
    } else if (!excludeFromOutput && func.fields.exists(f => enrichment(f).get.isExcludedFromOutput)) {
      val clone = copy()
      for (field <- func.fields if enrichment(field).get.isExcludedFromOutput) {
        clone.enrichment(field).get.excludeFromOutput(value = false)
      }
      clone
    } else {
      this
    }
  }

  private[enrich] def enrich[D](path: Seq[String], func: EnrichFunc[_, D], excludeFromOutput: Boolean): Enrichable = {
    if (path.isEmpty || (path.length == 1 && path.head == "")) enrich(func, excludeFromOutput)
    else {
      val field = this.field(path.head)
      enrichment(field) match {
        case Some(enrichable) =>
          val enriched = enrichable.enrich(path.tail, func, excludeFromOutput)
          if (enriched == enrichable) this
          else enrich(field, enriched)
        case None => this
      }
    }
  }

  def apply[D : ClassTag](path: Seq[String]): Option[TypedEnrichable[D]] = {
    if (path.isEmpty || (path.length == 1 && path.head == "")) Some(this.asInstanceOf[TypedEnrichable[D]])
    else {
      if (path.head == "") {
        val remaining = path.tail
        enrichment(remaining.head) match {
          case Some(child) => child(remaining.tail)
          case None => for (child <- _enrichments.values) {
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

  def apply[D : ClassTag](key: String): Option[TypedEnrichable[D]] = apply(SelectorUtil.parse(key))

  def get[D : ClassTag](path: String): Option[D] = get(SelectorUtil.parse(path))
  def get[D : ClassTag](path: Seq[String]): Option[D] = apply[D](path) match {
    case Some(enrichable) => Some(enrichable.get)
    case None => None
  }
}
