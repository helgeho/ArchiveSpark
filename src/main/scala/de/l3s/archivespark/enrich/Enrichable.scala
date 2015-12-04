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

import de.l3s.archivespark.utils.{Copyable, JsonConvertible}

import scala.reflect.ClassTag

trait Enrichable[T] extends Serializable with Copyable[Enrichable[T]] with JsonConvertible {
  private var excludeFromOutput: Option[Boolean] = None

  def get: T

  def isExcludedFromOutput: Boolean = excludeFromOutput match {
    case Some(value) => value
    case None => false
  }

  def excludeFromOutput(value: Boolean = true, overwrite: Boolean = true): Unit = excludeFromOutput match {
    case Some(v) => if (overwrite) excludeFromOutput = Some(v)
    case None => excludeFromOutput = Some(value)
  }

  private[enrich] var _enrichments = Map[String, Enrichable[_]]()
  def enrichments = _enrichments

  def apply[D : ClassTag](key: String): Option[Enrichable[D]] = {
    def find(current: Enrichable[_], path: Seq[String]): Enrichable[_] = {
      if (path.isEmpty || (path.length == 1 && path.head == "")) return current
      if (path.head == "") {
        var target = find(this, path.drop(1))
        if (target != null) return target
        for (e <- _enrichments.values) {
          target = find(e, path)
          if (target != null) return target
        }
        null
      } else {
        current.enrichments.get(path.head) match {
          case Some(enrichable) => find(enrichable, path.drop(1))
          case None => null
        }
      }
    }
    val target = find(this, key.trim.split("\\."))
    if (target == null) return None
    target.get match {
      case _: D => Some(target.asInstanceOf[Enrichable[D]])
      case _ => None
    }
  }

  def get[D : ClassTag](key: String): Option[D] = apply[D](key) match {
    case Some(enrichable) => Some(enrichable.get)
    case None => None
  }
}
