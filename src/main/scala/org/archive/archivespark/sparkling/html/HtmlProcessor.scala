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

import org.archive.archivespark.sparkling.Sparkling.prop
import org.archive.archivespark.sparkling.util.{Common, IteratorUtil, RegexUtil}

import scala.util.matching.Regex

object HtmlProcessor {
  var maxHtmlStackDepth: Int = prop(500)(maxHtmlStackDepth, maxHtmlStackDepth = _) // empirically determined, amazon.com ~ 180
  var strictMode: Boolean = prop(true)(strictMode, strictMode = _)

  val CodeTags: Set[String] = Set("script", "style", "textarea", "pre")
  val TagOpenClosePattern: Regex = """<([ /]*)([^<> ]+)(>| [^<>]*>)""".r

  case class TagMatch(tag: String, name: String, opening: Boolean, closing: Boolean, attributes: String, text: String, stack: List[String]) {
    def closingTag: Boolean = closing && !opening
    def openingTag: Boolean = opening && !closing
    def selfClosingTag: Boolean = opening && closing
  }

  def strictHtml(html: String): Option[String] = Some(html.trim).filter(h => h.startsWith("<") && h.endsWith(">"))

  def iterateTags(html: String): Iterator[TagMatch] = {
    val strict = if (!strictMode) Some(html) else strictHtml(html)
    if (strict.isDefined) {
      var pos = 0
      var stack = List.empty[String]
      var inCode: Option[String] = None
      val tags = collection.mutable.Map.empty[String, Int]
      for (tag <- TagOpenClosePattern.findAllMatchIn(strict.get)) yield {
        val slash = tag.group(1).trim
        val name = tag.group(2).trim.toLowerCase
        val opening = !slash.startsWith("/")
        val (attributes, closing) = {
          val attributes = tag.group(3).dropRight(1).trim
          if (attributes.endsWith("/")) (attributes.dropRight(1), true)
          else (attributes, !opening)
        }
        if (inCode.isDefined && name == inCode.get && closing && !opening) inCode = None
        if (inCode.isEmpty) {
          if (!opening || !closing) {
            if (opening) {
              stack +:= name
              if (stack.size > maxHtmlStackDepth) Common.printThrow("Max HTML stack depth reached.")
              tags.update(name, tags.getOrElse(name, 0) + 1)
              if (CodeTags.contains(name)) inCode = Some(name)
            } else if (tags.contains(name)) {
              val drop = stack.takeWhile(_ != name)
              stack = stack.drop(drop.size)
              for (dropTag <- drop ++ Iterator(stack.head)) {
                val count = tags(dropTag)
                if (count == 1) tags.remove(dropTag)
                else tags.update(dropTag, count - 1)
              }
              stack = stack.drop(1)
            }
          }
          val text = html.substring(pos, tag.start)
          pos = tag.end
          Some(TagMatch(tag.matched, name, opening, closing, attributes, text, stack))
        } else None
      }
    } else Iterator.empty
  }.flatten

  private def internalProcessTags(tags: BufferedIterator[TagMatch], handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true, ignoreHierarchical: Set[String] = Set.empty): Iterator[(TagMatch, Seq[TagMatch])] = {
    if (!tags.hasNext) return Iterator.empty
    var iter = tags
    IteratorUtil.whileDefined {
      if (iter.hasNext) Some {
        val head = iter.head
        val name = head.name
        iter.next()
        if (head.opening) {
          val activeHandlers = handlers.filter(_.handles(name))
          if (activeHandlers.isEmpty) Iterator.empty
          else {
            val (hierarchicalHandlers, flatHandlers) = activeHandlers.partition(_.hierarchical)
            for (handler <- flatHandlers) handler.handle(head, Seq.empty)
            if (hierarchicalHandlers.nonEmpty && !ignoreHierarchical.contains(name)) {
              if (head.closing) {
                for (handler <- hierarchicalHandlers) handler.handle(head, Seq.empty)
                Iterator((head, Seq.empty))
              } else {
                val lvl = head.stack.size
                val tail = IteratorUtil.takeUntil(iter, including = true) { t => t.stack.size < lvl }.toSeq
                val innerTags = if (tail.isEmpty) Seq.empty else {
                  val last = tail.last
                  val children = if (last.closingTag && last.name == name) tail.dropRight(1) else tail
                  internalProcessTags(children.toIterator.buffered, handlers, outer, inner, if (inner) ignoreHierarchical else ignoreHierarchical + name).toSeq
                }
                if (outer || !innerTags.exists { case (t, _) => t.name == name }) {
                  for (handler <- hierarchicalHandlers) handler.handle(head, tail)
                  Iterator((head, tail)) ++ innerTags
                } else innerTags
              }
            } else if (flatHandlers.nonEmpty) Iterator((head, Seq.empty))
            else Iterator.empty
          }
        } else {
          val activeHandlers = handlers.filter(h => h.handleClosing && h.handles(name))
          if (activeHandlers.isEmpty) Iterator.empty
          else {
            for (handler <- activeHandlers) handler.handle(head, Seq.empty)
            Iterator((head, Seq.empty))
          }
        }
      } else None
    }
  }.flatten

  def lazyProcessTags(tags: TraversableOnce[TagMatch], handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, Seq[TagMatch])] = {
    internalProcessTags(tags.toIterator.buffered, handlers, outer, inner)
  }

  def lazyProcess(html: String, handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, Seq[TagMatch])] = {
    lazyProcessTags(iterateTags(html), handlers, outer, inner)
  }

  def processTags(tags: TraversableOnce[TagMatch], handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true): Int = {
    lazyProcessTags(tags, handlers, outer, inner).size
  }

  def process(html: String, handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true): Int = {
    lazyProcess(html, handlers, outer, inner).size
  }

  def encloseTags(tags: TraversableOnce[TagMatch], names: Set[String], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, Seq[TagMatch])] = {
    lazyProcessTags(tags, Set(TagHandler.noop(names)), outer, inner)
  }

  def text(children: TraversableOnce[TagMatch]): String = children.filter(t => !(CodeTags.contains(t.name) && t.closing)).map(_.text).mkString

  def textHandler(tags: Set[String]): TagHandler[Seq[(TagMatch, String)]] = {
    TagHandler(tags, Seq.empty[(TagMatch, String)])((tag, children, r) => r ++ Seq((tag, HtmlProcessor.text(children))))
  }

  def childrenHandler(tags: Set[String]): TagHandler[Seq[(TagMatch, Seq[TagMatch])]] = {
    TagHandler(tags, Seq.empty[(TagMatch, Seq[TagMatch])])((tag, children, r) => r ++ Seq((tag, children)))
  }

  def tagsWithText(html: String, names: Set[String], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, String)] = tagsWithChildren(html, names, outer, inner).map { case (tag, children) =>
    (tag, text(children))
  }

  def tagsWithChildren(html: String, names: Set[String], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, Seq[TagMatch])] = {
    encloseTags(iterateTags(html), names, outer, inner)
  }

  def printTags(html: String, names: Set[String], outer: Boolean = true, inner: Boolean = true): Iterator[String] = {
    tagsWithChildren(html, names, outer, inner).map{case (tag, children) => print(tag, children)}
  }

  def tags(html: String, names: Set[String]): Iterator[TagMatch] = {
    val lower = names.map(_.toLowerCase)
    iterateTags(html).filter { t => t.opening && lower.contains(t.name) }
  }

  def tag(html: String, name: String): Iterator[String] = RegexUtil.r(s"""(?i)< *$name(>| [^>]*>)""").findAllIn(html)

  def attributeValue(tag: String, attribute: String): Option[String] = {
    RegexUtil.r(s"""(?i)(^|\\W)$attribute *= *('[^']*'|"[^"]*")""").findFirstMatchIn(tag).map(_.group(2).drop(1).dropRight(1))
  }

  def attributeValues(html: String, tagName: String, attribute: String): Iterator[String] = tag(html, tagName).flatMap { tag =>
    attributeValue(tag, attribute)
  }

  def print(tag: TagMatch, children: TraversableOnce[TagMatch]): String = tag.tag + children.map(t => t.text + t.tag).mkString

  def print(tags: TraversableOnce[TagMatch]): String = {
    val iter = tags.toIterator
    if (iter.hasNext) print(iter.next, iter) else ""
  }
}
