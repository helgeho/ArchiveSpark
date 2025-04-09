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

package org.archive.webservices.archivespark.functions

import org.archive.webservices.archivespark.model._
import org.archive.webservices.archivespark.model.dataloads.ByteLoad
import org.archive.webservices.sparkling.html.HtmlProcessor
import org.archive.webservices.sparkling.util.IteratorUtil

object HtmlNamespace {
  def get: EnrichFunc.Basic[ByteLoad.Root, String, String] = StringContent.mapIdentity("html")
}

object Html extends HtmlTag("html", 0, "html") {
  def apply(tagName: String): HtmlTags = all(tagName)
  def apply(tagName: String, fieldName: String): HtmlTags = all(tagName, fieldName)
  def apply(tagName: String, index: Int): HtmlTag = apply(tagName, index, tagName)
  def apply(tagName: String, index: Int, fieldName: String): HtmlTag = new HtmlTag(tagName, index, fieldName)

  def first(selector: String): HtmlTag = first(selector, selector)
  def first(selector: String, fieldName: String): HtmlTag = new HtmlTag(selector, 0, fieldName)

  def all(selector: String): HtmlTags = all(selector, selector)
  def all(selector: String, fieldName: String): HtmlTags = new HtmlTags(selector, fieldName)
}

class HtmlTag(tagName: String, index: Int, field: String) extends BoundEnrichFunc[ByteLoad.Root, EnrichRoot, String, String, String](HtmlNamespace.get) {
  override def fields: Seq[String] = Seq(field)

  override def deriveBound(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    val tag = IteratorUtil.last(HtmlProcessor.printTags(source.get, Set(tagName)).take(index + 1).zipWithIndex)
    if (tag.isDefined && tag.get._2 == index) derivatives << tag.get._1
  }
}

class HtmlTags(tagName: String, field: String) extends BoundMultiEnrichFunc[ByteLoad.Root, EnrichRoot, String, String, String](HtmlNamespace.get) {
  override def fields: Seq[String] = Seq(field)

  override def deriveBound(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    derivatives.setNext(MultiValueEnrichable(HtmlProcessor.printTags(source.get, Set(tagName)).toList))
  }
}
