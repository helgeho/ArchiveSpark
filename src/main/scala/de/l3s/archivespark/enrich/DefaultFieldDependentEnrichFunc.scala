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

trait DefaultFieldDependentEnrichFunc[Root <: EnrichRoot[_, _], Source <: Enrichable[_, _], DefaultFieldType] extends DependentEnrichFunc[Root, Source]
  with DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] {
  def defaultField: String

  override def on(dependency: EnrichFunc[Root, _], field: String): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = new PipedDependentEnrichFuncWithDefaultField[Root, Source, DefaultFieldType](this, dependency, field)
  override def on(dependency: DefaultFieldEnrichFunc[Root, _, _]): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(dependency, dependency.defaultField)

  override def on(dependency: EnrichFunc[Root, _], field: String, index: Int): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(dependency, field + s"[$index]")
  override def on(dependency: DefaultFieldEnrichFunc[Root, _, _], index: Int): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(dependency, dependency.defaultField, index)

  override def onEach(dependency: EnrichFunc[Root, _], field: String): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(dependency, field + "*")
  override def onEach(dependency: DefaultFieldEnrichFunc[Root, _, _]): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = onEach(dependency, dependency.defaultField)

  override def of(dependency: EnrichFunc[Root, _], field: String): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(dependency, field)
  override def of(dependency: DefaultFieldEnrichFunc[Root, _, _]): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = on(dependency)
  override def of(dependency: EnrichFunc[Root, _], field: String, index: Int): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = of(dependency, field, index)
  override def of(dependency: DefaultFieldEnrichFunc[Root, _, _], index: Int): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = of(dependency, index)
  override def ofEach(dependency: EnrichFunc[Root, _], field: String): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = onEach(dependency, field)
  override def ofEach(dependency: DefaultFieldEnrichFunc[Root, _, _]): DefaultFieldEnrichFunc[Root, Source, DefaultFieldType] = onEach(dependency)
}
