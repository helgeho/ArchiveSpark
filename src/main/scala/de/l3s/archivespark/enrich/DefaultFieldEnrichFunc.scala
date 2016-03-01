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

package de.l3s.archivespark.enrich

import de.l3s.archivespark.utils.SelectorUtil

trait DefaultFieldEnrichFunc[Root <: EnrichRoot[_, _], Source <: Enrichable[_, _], DefaultFieldType] extends EnrichFunc[Root, Source] {
  def defaultField: String

  override def on(source: Seq[String]): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = new PipedEnrichFuncWithDefaultField[Root, Source, DefaultFieldType](this, source)

  override def on(source: String): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(SelectorUtil.parse(source))
  override def on(source: String, index: Int): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(SelectorUtil.parse(source), index)
  override def on(source: Seq[String], index: Int): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(source :+ s"[$index]")
  override def onEach(source: String): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = onEach(SelectorUtil.parse(source))
  override def onEach(source: Seq[String]): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(source :+ "*")

  override def of(source: String): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(source)
  override def of(source: Seq[String]): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(source)
  override def of(source: String, index: Int): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(source, index)
  override def of(source: Seq[String], index: Int): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(source, index)
  override def ofEach(source: String): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = onEach(source)
  override def ofEach(source: Seq[String]): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = onEach(source)
}
