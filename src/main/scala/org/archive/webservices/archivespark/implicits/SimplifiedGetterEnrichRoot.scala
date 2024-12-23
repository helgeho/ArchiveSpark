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

package org.archive.webservices.archivespark.implicits

import org.archive.webservices.archivespark.model.EnrichRoot
import org.archive.webservices.archivespark.model.pointers.FieldPointer

import scala.reflect.ClassTag

class SimplifiedGetterEnrichRoot[Root <: EnrichRoot](root: Root) {
  def value[R >: Root <: EnrichRoot, T](pointer: FieldPointer[R, T]): Option[T] = pointer.get(root)

  def getValue[R >: Root <: EnrichRoot, T](pointer: FieldPointer[R, T]): T = value(pointer).get

  def valueOrElse[R >: Root <: EnrichRoot, T : ClassTag](pointer: FieldPointer[R, T], elseValue: => T): T = value(pointer).getOrElse(elseValue)
}
