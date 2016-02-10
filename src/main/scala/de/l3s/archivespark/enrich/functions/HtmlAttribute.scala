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
import de.l3s.archivespark.enrich._
import de.l3s.archivespark.utils.IdentityMap
import org.jsoup.parser.Parser

import scala.collection.JavaConverters._

private object HtmlAttributeNamespace extends IdentityEnrichFunction[String](Html, "html", "attributes")

object HtmlAttribute {
  def apply(name: String): HtmlAttribute = new HtmlAttribute(name)
}

class HtmlAttribute private (attribute: String) extends BoundEnrichFunc(HtmlAttributeNamespace) {
  override def dependencyField: String = HtmlAttributeNamespace.fieldName

  override def fields: Seq[String] = Seq(attribute)
  override def field: IdentityMap[String] = IdentityMap(
    "value" -> attribute
  )

  override def derive(source: Enrichable[String], derivatives: Derivatives[Enrichable[_]]): Unit = {
    val nodes = Parser.parseXmlFragment(source.get, "").asScala
    if (nodes.nonEmpty) {
      val el = nodes.head
      val lc = attribute.toLowerCase
      el.attributes().asScala.find(a => a.getKey.toLowerCase == lc) match {
        case Some(a) => derivatives << ArchiveRecordField(a.getValue)
        case None => // skip
      }
    }
  }
}
