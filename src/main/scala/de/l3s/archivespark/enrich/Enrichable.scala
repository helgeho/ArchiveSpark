/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

import de.l3s.archivespark.utils.{SelfTyped, SelectorUtil, Copyable, JsonConvertible}

import scala.reflect.ClassTag

trait Enrichable[T, This <: Enrichable[_, _]] extends Serializable with Copyable[Enrichable[T, This]] with JsonConvertible with SelfTyped[This] {
  private var excludeFromOutput: Option[Boolean] = None

  def get: T

  def isExcludedFromOutput: Boolean = excludeFromOutput match {
    case Some(value) => value
    case None => false
  }

  private def excludeFromOutput(value: Boolean = true, overwrite: Boolean = true): Unit = excludeFromOutput match {
    case Some(v) => if (overwrite) excludeFromOutput = Some(v)
    case None => excludeFromOutput = Some(value)
  }

  private[archivespark] var _parent: Enrichable[_, _] = null
  def parent[A] = _parent.asInstanceOf[Enrichable[A, _]]

  private[archivespark] var _root: EnrichRoot[_, _] = null
  def root[A] = _root.asInstanceOf[Enrichable[A, _]]

  private var _enrichments = Map[String, Enrichable[_, _]]()
  def enrichments = _enrichments.keySet

  def enrichment(key: String) = _enrichments.get(key)

  def enrich(fieldName: String, enrichment: Enrichable[_, _]): This = {
    val clone = copy()
    clone._enrichments = _enrichments.updated(fieldName, enrichment)
    clone.asInstanceOf[This]
  }

  def enrich(func: EnrichFunc[_, This], excludeFromOutput: Boolean = false): This = {
    if (func.exists(self)) return self
    val derivatives = new Derivatives(func.fields)
    func.derive(self, derivatives)
    val clone = copy()
    for ((field, enrichment) <- derivatives.get) {
      enrichment._root = _root
      enrichment._parent = this
      enrichment.excludeFromOutput(excludeFromOutput, overwrite = false)
      clone._enrichments = _enrichments.updated(field, enrichment)
    }
    clone.asInstanceOf[This]
  }

  def enrich(path: Seq[String], func: EnrichFunc[_, _], excludeFromOutput: Boolean): This = {
    if (path.isEmpty) enrich(func.asInstanceOf[EnrichFunc[_, This]], excludeFromOutput)
    else {
      val field = path.head
      apply(field) match {
        case Some(enrichable) =>
          val enriched = enrichable.enrich(path.tail, func, excludeFromOutput).asInstanceOf[Enrichable[_, _]]
          if (enriched == enrichable) self
          else enrich(field, enriched)
        case None => self
      }
    }
  }

  def apply[D : ClassTag](path: Seq[String]): Option[Enrichable[D, _]] = {
    if (path.isEmpty || (path.length == 1 && path.head == "")) Some(this.asInstanceOf[Enrichable[D, This]])
    else {
      if (path.head == "") {
        val remaining = path.tail
        enrichment(remaining.head) match {
          case Some(child) => child(remaining.tail)
          case None => for (child <- _enrichments.values) {
              val target: Option[Enrichable[D, _]] = child[D](path)
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

  def apply[D : ClassTag](key: String): Option[Enrichable[D, _]] = apply(SelectorUtil.parse(key))

  def get[D : ClassTag](path: String): Option[D] = get(SelectorUtil.parse(path))
  def get[D : ClassTag](path: Seq[String]): Option[D] = apply[D](path) match {
    case Some(enrichable) => Some(enrichable.get)
    case None => None
  }
}
