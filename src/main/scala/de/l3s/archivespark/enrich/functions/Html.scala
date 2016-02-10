/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.enrich.functions

import de.l3s.archivespark.ArchiveRecordField
import de.l3s.archivespark.cdx.ResolvedCdxRecord
import de.l3s.archivespark.enrich._
import de.l3s.archivespark.utils.IdentityMap
import org.jsoup.Jsoup

private object HtmlNamespace extends IdentityEnrichFunction[String](StringContent, "text", "html")

object Html extends Html("body", 0, "body") {
  def apply(selector: String, index: Int, fieldName: String): Html = new Html(selector, index, fieldName)
  def apply(selector: String, index: Int): Html = new Html(selector, index, selector)
  def apply(selector: String): Html = new Html(selector, 0, selector)
  def apply(selector: String, fieldName: String): Html = new Html(selector, 0, fieldName)
}

class Html private (selector: String, index: Int, fieldName: String) extends BoundEnrichFunc(HtmlNamespace) {
  override def dependencyField: String = HtmlNamespace.fieldName

  override def fields: Seq[String] = Seq(fieldName)
  override def field: IdentityMap[String] = IdentityMap(
    "html" -> fieldName
  )

  override def derive(source: Enrichable[String], derivatives: Derivatives[Enrichable[_]]): Unit = {
    val url = source.root[ResolvedCdxRecord].get.originalUrl
    val doc = Jsoup.parse(source.get, url)
    val elements = doc.select(selector)
    if (elements.size() > index) derivatives << ArchiveRecordField(elements.get(index).toString)
  }
}
