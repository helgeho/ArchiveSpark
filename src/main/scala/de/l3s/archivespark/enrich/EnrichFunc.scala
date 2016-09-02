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

import de.l3s.archivespark.utils.SelectorUtil

trait EnrichFunc[Root <: EnrichRoot, Source] extends Serializable {
  def source: Seq[String]
  def fields: Seq[String]

  def pathTo(field: String) = source ++ SelectorUtil.parse(field)

  def enrich(root: Root): Root = enrich(root, excludeFromOutput = false)

  private[enrich] def enrich(root: Root, excludeFromOutput: Boolean): Root = root.enrich(source, this, excludeFromOutput).asInstanceOf[Root]

  protected[enrich] def derive(source: TypedEnrichable[Source], derivatives: Derivatives): Unit

  def aliases: Map[String, String] = Map()

  def isEnriched(root: Root): Boolean = root(source) match {
    case Some(source) => exists(source)
    case None => false
  }

  def exists(source: Enrichable): Boolean = fields.forall(f => source.enrichment(f).isDefined)
}