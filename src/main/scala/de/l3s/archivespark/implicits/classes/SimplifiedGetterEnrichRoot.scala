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

import de.l3s.archivespark.enrich.{DefaultFieldAccess, EnrichFunc, EnrichRoot}
import de.l3s.archivespark.utils.SelectorUtil

import scala.reflect.ClassTag

class SimplifiedGetterEnrichRoot[Root <: EnrichRoot](root: Root) {
  def getValue[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultFieldAccess[_, T]): T = value(f).get
  def getValue[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String): T = value(f, field).get

  def value[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultFieldAccess[_, T]): Option[T] = root.get[T](f.pathToDefaultField)
  def value[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String): Option[T] = root.get[T](f.pathTo(field))

  def valueOrElse[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _] with DefaultFieldAccess[_, T], elseValue: => T): T = root.get[T](f.pathToDefaultField).getOrElse(elseValue)
  def valueOrElse[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](f: EnrichFunc[SpecificRoot, _], field: String, elseValue: => T): T = root.get[T](f.pathTo(field)).getOrElse(elseValue)
}
