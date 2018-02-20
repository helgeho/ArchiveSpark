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

package de.l3s.archivespark.enrich.functions

import de.l3s.archivespark.enrich._
import de.l3s.archivespark.enrich.dataloads.ByteContentLoad
import org.jsoup.parser.Parser

import scala.collection.JavaConverters._

private object HtmlAttributeNamespace extends IdentityEnrichFunction(Html, "attributes")

object HtmlAttribute {
  def apply(name: String): HtmlAttribute = new HtmlAttribute(name)
}

class HtmlAttribute private (attribute: String) extends BoundEnrichFuncWithDefaultField[EnrichRoot with ByteContentLoad, String, String](HtmlAttributeNamespace) with SingleField[String] {
  override def fields = Seq(attribute)
  override def aliases = Map("value" -> attribute)

  override def derive(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    val nodes = Parser.parseXmlFragment(source.get, "").asScala
    if (nodes.nonEmpty) {
      val el = nodes.head
      val lc = attribute.toLowerCase
      el.attributes().asScala.find(a => a.getKey.toLowerCase == lc) match {
        case Some(a) => derivatives << a.getValue
        case None => // skip
      }
    }
  }
}
