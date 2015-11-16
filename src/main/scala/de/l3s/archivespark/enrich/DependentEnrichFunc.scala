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

trait DependentEnrichFunc[Root <: EnrichRoot[_], Source <: Enrichable[_]] extends EnrichFunc[Root, Source] {
  def dependency: EnrichFunc[Root, _]

  def dependencyField: String

  def source: Seq[String] = dependency.source :+ dependency.field(dependencyField)

  def on(dependency: EnrichFunc[Root, _]): DependentEnrichFunc[Root, Source] = new PipedEnrichFunc[Root, Source](this, dependency)

  override private[enrich] def enrich(root: Root, excludeFromOutput: Boolean): Root = {
    val rootWithDependency = if (dependency.exists(root)) root else dependency.enrich(root, excludeFromOutput = true)
    super.enrich(rootWithDependency, excludeFromOutput)
  }

  override def exists(root: Root): Boolean = {
    if (!dependency.exists(root)) false
    else super.exists(root)
  }
}
