/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2024 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.model.pointers

import org.apache.spark.sql
import org.apache.spark.sql.Column
import org.archive.webservices.archivespark.model._
import org.archive.webservices.archivespark.util.SelectorUtil
import org.archive.webservices.archivespark.model.{EnrichRoot, TypedEnrichRoot}
import org.archive.webservices.archivespark.util.SelectorUtil

trait GenericFieldPointer[-R <: EnrichRoot, +T] extends Serializable { this: FieldPointer[_, _] =>
}

trait FieldPointer[-Root <: EnrichRoot, T] extends GenericFieldPointer[Root, T] {
  def dependencyPath: Seq[FieldPointer[Root, _]] = Seq(parent)

  def path[R <: Root](root: EnrichRootCompanion[R]): Seq[String]

  def get[R <: Root](root: R): Option[T] = enrichable(root).map(_.get)

  def exists[R <: Root](root: R): Boolean = root[T](path(root)).isDefined

  def enrichable[R <: Root](root: R): Option[TypedEnrichable[T]] = {
    val initialized = init(root, excludeFromOutput = false)
    initialized[T](path(initialized))
  }

  def multi: MultiFieldPointer[Root, T] = new SingleToMultiFieldPointer[Root, T](this)

  def init[R <: Root](root: R, excludeFromOutput: Boolean): R = root

  def pathTo[R <: Root](root: EnrichRootCompanion[R], field: String): Seq[String] = path(root) ++ SelectorUtil.parse(field)

  def col[R <: Root](root: EnrichRootCompanion[R]): Column = sql.functions.col(SelectorUtil.toString(path(root).filter(f => f != "*" && !f.startsWith("["))))

  def parent[A]: FieldPointer[Root, A] = new RelativeFieldPointer(this, 1, Seq.empty)

  def child[A](field: String): FieldPointer[Root, A] = new RelativeFieldPointer(this, 0, Seq(field))

  def sibling[A](field: String): FieldPointer[Root, A] = new RelativeFieldPointer(this, 1, Seq(field))

  def mapEnrichable[A](field: String)(f: TypedEnrichable[T] => A): EnrichFunc[Root, T, A] = {
    val sourcePointer = this
    new EnrichFunc[Root, T, A] {
      override def source: FieldPointer[Root, T] = sourcePointer
      override def fields: Seq[String] = Seq(field)
      override def derive(source: TypedEnrichable[T], derivatives: Derivatives): Unit = {
        derivatives << f(source)
      }
    }
  }

  def map[A](field: String)(f: T => A): EnrichFunc[Root, T, A] = mapEnrichable(field)(e => f(e.get))

  def mapMultiEnrichable[A](field: String)(f: TypedEnrichable[T] => Seq[A]): MultiEnrichFunc[Root, T, A] = {
    val sourcePointer = this
    new MultiEnrichFunc[Root, T, A] {
      override def source: FieldPointer[Root, T] = sourcePointer
      override def fields: Seq[String] = Seq(field)
      override def derive(source: TypedEnrichable[T], derivatives: Derivatives): Unit = {
        derivatives.setNext(MultiValueEnrichable(f(source)))
      }
    }
  }

  def mapMulti[A](field: String)(f: T => Seq[A]): MultiEnrichFunc[Root, T, A] = mapMultiEnrichable(field)(e => f(e.get))

  def mapIdentity(field: String): EnrichFunc[Root, T, T] = {
    val sourcePointer = this
    new EnrichFunc[Root, T, T] {
      override def source: FieldPointer[Root, T] = sourcePointer
      override def fields: Seq[String] = Seq(field)
      override def derive(source: TypedEnrichable[T], derivatives: Derivatives): Unit = {
        derivatives.setNext(IdentityField[T])
      }
    }
  }
}

object FieldPointer {
  def apply[Root <: EnrichRoot, T](path: String): FieldPointer[Root, T] = apply(SelectorUtil.parse(path))
  def apply[Root <: EnrichRoot, T](path: Seq[String]): FieldPointer[Root, T] = new PathFieldPointer(path)

  def multi[Root <: EnrichRoot, T](path: String): MultiFieldPointer[Root, T] = multi(SelectorUtil.parse(path))
  def multi[Root <: EnrichRoot, T](path: Seq[String]): MultiFieldPointer[Root, T] = apply(path).multi

  def root[Root <: TypedEnrichRoot[T], T]: FieldPointer[Root, T] = new PathFieldPointer(Seq.empty)
}