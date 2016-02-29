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

import de.l3s.archivespark.utils.{SelectorUtil, IdentityMap}

trait EnrichFunc[Root <: EnrichRoot[_, _], Source <: Enrichable[_, _]] extends Serializable {
  def source: Seq[String] = Seq()
  def fields: Seq[String]

  def enrich(root: Root): Root = enrich(root, excludeFromOutput = false)

  def on(source: String): EnrichFunc[Root, Source] = on(SelectorUtil.parse(source))
  def on(source: Seq[String]): EnrichFunc[Root, Source] = new PipedEnrichFunc[Root, Source](this, source)

  def on(source: String, index: Int): EnrichFunc[Root, Source] = on(SelectorUtil.parse(source), index)
  def on(source: Seq[String], index: Int): EnrichFunc[Root, Source] = on(source :+ s"[$index]")

  def onAll(source: String): EnrichFunc[Root, Source] = onAll(SelectorUtil.parse(source))
  def onAll(source: Seq[String]): EnrichFunc[Root, Source] = on(source :+ "*")

  protected[enrich] def enrich(root: Root, excludeFromOutput: Boolean): Root = root.enrich(source, this, excludeFromOutput).asInstanceOf[Root]

  def derive(source: Source, derivatives: Derivatives): Unit

  def field: IdentityMap[String] = IdentityMap[String]()

  def exists(root: Root): Boolean = root(source) match {
    case Some(source: Source) => exists(source)
    case None => false
  }

  def exists(source: Source): Boolean = fields.forall(f => source(f).isDefined)
}
