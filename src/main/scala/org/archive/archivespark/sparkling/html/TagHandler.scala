/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.archivespark.sparkling.html

import org.archive.archivespark.sparkling.html.HtmlProcessor.TagMatch

trait TagHandler[R] {
  def handles(tag: String): Boolean
  def handle(tag: TagMatch, children: Seq[TagMatch]): Unit
  def result: R
  def hierarchical: Boolean
  def handleClosing: Boolean
}

private class NoopTagHandler (val tags: Set[String]) extends TagHandler[Boolean] {
  override def handles(tag: String): Boolean = tags.contains(tag)
  override def handle(tag: TagMatch, children: Seq[TagMatch]): Unit = {}
  override val result: Boolean = true
  override val hierarchical: Boolean = true
  override val handleClosing: Boolean = false
}

private class AllTagHandler[R] (val hierarchical: Boolean, val handleClosing: Boolean, initial: R, handler: (TagMatch, Seq[TagMatch], R) => R) extends TagHandler[R] {
  var currentResult: R = initial
  override def handles(tag: String): Boolean = true
  override def handle(tag: TagMatch, children: Seq[TagMatch]): Unit = currentResult = handler(tag, children, currentResult)
  override def result: R = currentResult
}

private class SpecificTagHandler[R](val tags: Set[String], hierarchical: Boolean, handleClosing: Boolean, initial: R, handler: (TagMatch, Seq[TagMatch], R) => R) extends AllTagHandler[R](hierarchical, handleClosing, initial, handler) {
  override def handles(tag: String): Boolean = tags.contains(tag)
}

object TagHandler {
  def noop(tags: Set[String]): TagHandler[_] = new NoopTagHandler(tags.map(_.toLowerCase))
  def all[R](initial: R, hierarchical: Boolean = false, handleClosing: Boolean = false)(handler: (TagMatch, Seq[TagMatch], R) => R): TagHandler[R] = new AllTagHandler(hierarchical, handleClosing, initial, handler)
  def apply[R](tags: Set[String], initial: R, hierarchical: Boolean = true, handleClosing: Boolean = false)(handler: (TagMatch, Seq[TagMatch], R) => R): TagHandler[R] = new SpecificTagHandler(tags.map(_.toLowerCase), hierarchical, handleClosing, initial, handler)
}