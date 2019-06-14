/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2018 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package org.archive.archivespark.functions

import org.archive.archivespark.model._
import org.archive.archivespark.model.dataloads.ByteLoad
import org.archive.archivespark.model.pointers.DependentFieldPointer
import org.archive.archivespark.sparkling.html.HtmlProcessor
import org.archive.archivespark.sparkling.util.IteratorUtil

object FastHtmlNamespace {
  def get: DependentFieldPointer[ByteLoad.Root, String] = StringContent.mapIdentity("html").get[String]("html")
}

object FastHtml extends HtmlTag("body", 0, "body") {
  def apply(tagName: String): FastHtmlTags = all(tagName)
  def apply(tagName: String, fieldName: String): FastHtmlTags = all(tagName, fieldName)
  def apply(tagName: String, index: Int): FastHtmlTag = apply(tagName, index, tagName)
  def apply(tagName: String, index: Int, fieldName: String): FastHtmlTag = new FastHtmlTag(tagName, index, fieldName)

  def first(selector: String): FastHtmlTag = first(selector, selector)
  def first(selector: String, fieldName: String): FastHtmlTag = new FastHtmlTag(selector, 0, fieldName)

  def all(selector: String): FastHtmlTags = all(selector, selector)
  def all(selector: String, fieldName: String): FastHtmlTags = new FastHtmlTags(selector, fieldName)
}

class FastHtmlTag (tagName: String, index: Int, field: String) extends BoundEnrichFunc[ByteLoad.Root, String, String](FastHtmlNamespace.get) {
  override def fields: Seq[String] = Seq(field)

  override def derive(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    val tag = IteratorUtil.last(HtmlProcessor.tag(source.get, tagName).take(index + 1).zipWithIndex)
    if (tag.isDefined && tag.get._2 == index) derivatives << tag.get._1
  }
}

class FastHtmlTags (tagName: String, field: String) extends BoundMultiEnrichFunc[ByteLoad.Root, String, String](FastHtmlNamespace.get) {
  override def fields: Seq[String] = Seq(field)

  override def derive(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    derivatives.setNext(MultiValueEnrichable(HtmlProcessor.tag(source.get, tagName).toList))
  }
}
