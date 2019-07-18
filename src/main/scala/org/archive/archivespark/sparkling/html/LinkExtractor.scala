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

import java.net.URL

import org.archive.archivespark.sparkling.html.HtmlProcessor.TagMatch
import org.archive.archivespark.sparkling.util.{RegexUtil, StringUtil}

import scala.util.Try
import scala.util.matching.Regex

object LinkExtractor {
  val BaseTag = "base"
  val LinkTags: Set[String] = Set("a", "form")
  val ContentEmbedTags: Set[String] = Set("applet", "area", "embed", "frame", "iframe", "img", "input", "object", "source")
  val EmbedTags: Set[String] = ContentEmbedTags ++ Set("link", "script")
  val CssUrlPattern: Regex = """url\s*\(\s*([\\"']*.+?[\\"']*)\s*\)""".r
  val CssImportNoUrlPattern: Regex = """@import\s+(('[^']+')|("[^"]+")|(\('[^']+'\))|(\("[^"]+"\))|(\([^)]+\))|([a-z0-9_.:/\\-]+))\s*;""".r

  def baseUrl(html: String, url: Option[String] = None): Option[String] = {
    HtmlProcessor.tag(html, BaseTag).toSeq.headOption.flatMap(HtmlProcessor.attributeValue(_, "href")).flatMap(resolveLink(_, url)).orElse(url)
  }

  def isValidLinkProtocol(url: String): Boolean = {
    val lowerCase = url.trim.toLowerCase
    lowerCase.startsWith("http:") || lowerCase.startsWith("https:") || lowerCase.startsWith("ftp:")
  }

  def resolveLink(url: String, base: Option[String] = None): Option[String] = {
    if (RegexUtil.matchesAbsoluteUrlStart(url)) {
      if (isValidLinkProtocol(url)) Try(new URL(url.trim).toString.trim).toOption
      else None
    } else base match {
      case Some(b) =>
        if (isValidLinkProtocol(b)) Try(new URL(new URL(b.trim), url.trim).toString.trim).toOption
        else None
      case None => None
    }
  }.map(_.replace(" ", "%20").replace("\n", "%0A").replace("\r", "%0D").replace("\t", "%09"))

  def tagTargets(tag: TagMatch, url: Option[String] = None): Set[String] = {
    tag.name match {
      case "a" =>
        HtmlProcessor.attributeValue(tag.attributes, "href").flatMap(resolveLink(_, url)).toIterator
      case "form" =>
        HtmlProcessor.attributeValue(tag.attributes, "action").flatMap(resolveLink(_, url)).toIterator
      case "applet" =>
        val files = HtmlProcessor.attributeValue(tag.attributes, "archive").map(_.split("[ ,]").toSeq).getOrElse {
          HtmlProcessor.attributeValue(tag.attributes, "code").orElse {
            HtmlProcessor.attributeValue(tag.attributes, "object")
          }.toSeq
        }
        val codebase = HtmlProcessor.attributeValue(tag.attributes, "codebase").map(resolveLink(_, url)).getOrElse(url)
        files.flatMap(resolveLink(_, codebase))
      case "object" =>
        val files = HtmlProcessor.attributeValue(tag.attributes, "archive").map(_.split("[ ,]").toSeq).getOrElse {
          HtmlProcessor.attributeValue(tag.attributes, "data").orElse {
            HtmlProcessor.attributeValue(tag.attributes, "classid")
          }.toSeq
        }
        val codebase = HtmlProcessor.attributeValue(tag.attributes, "codebase").map(resolveLink(_, url)).getOrElse(url)
        files.flatMap(resolveLink(_, codebase))
      case "area" =>
        HtmlProcessor.attributeValue(tag.attributes, "href").flatMap(resolveLink(_, url)).toIterator
      case "embed" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "frame" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "iframe" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "img" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "input" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "source" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "link" =>
        if (HtmlProcessor.attributeValue(tag.attributes, "rel").map(_.toLowerCase).contains("stylesheet")) {
          HtmlProcessor.attributeValue(tag.attributes, "href").flatMap(resolveLink(_, url)).toIterator
        } else Iterator.empty
      case "script" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case _ => Iterator.empty
    }
  }.toSet

  def outLinksWithChildren(html: String, url: Option[String] = None): Iterator[(String, TagMatch, Seq[TagMatch])] = {
    val base = baseUrl(html, url)
    HtmlProcessor.tagsWithChildren(html, LinkTags).flatMap { case (tag, children) =>
      tagTargets(tag, base).map((_, tag, children))
    }
  }

  def outLinks(html: String, url: Option[String] = None): Iterator[String] = {
    val base = baseUrl(html, url)
    HtmlProcessor.tags(html, LinkTags).flatMap(tagTargets(_, base))
  }

  def outLinksWithText(html: String, url: Option[String] = None): Iterator[(String, String, TagMatch)] = outLinksWithChildren(html, url).map { case (target, tag, children) =>
    (target, if (tag.name == "a") HtmlProcessor.text(children) else "", tag)
  }

  def outLinksHandler(html: String, url: Option[String] = None): TagHandler[Seq[(String, String, TagMatch)]] = {
    val base = baseUrl(html, url)
    TagHandler(LinkTags, Seq.empty[(String, String, TagMatch)]) { (tag, children, result) =>
      result ++ tagTargets(tag, base).map((_, if (tag.name == "a") HtmlProcessor.text(children) else "", tag))
    }
  }

  def targetLabelHandler(html: String, url: Option[String] = None, tag: String, targetAttribute: String, labelAttributes: Set[String], includeText: Boolean = true): TagHandler[Set[(String, String)]] = {
    val base = baseUrl(html, url)
    TagHandler(Set(tag), Set.empty[(String, String)], hierarchical = false) { (tag, children, result) =>
      val targetUrls = HtmlProcessor.attributeValue(tag.attributes, targetAttribute).flatMap(LinkExtractor.resolveLink(_, base)).toSet
      result ++ targetUrls.flatMap { dst =>
        labelAttributes.flatMap { attribute =>
          HtmlProcessor.attributeValue(tag.attributes, attribute).map(alt => (dst, alt))
        } ++ (if (includeText) Set((dst, HtmlProcessor.text(children))) else Set.empty)
      }
    }
  }

  def cssEmbeds(css: String, url: Option[String] = None): Set[String] = {
    for (m <- CssUrlPattern.findAllMatchIn(css) ++ CssImportNoUrlPattern.findAllMatchIn(css)) yield {
      resolveLink(StringUtil.stripBrackets(StringUtil.stripBracket(m.group(1), "(", ")"), Seq("\\", "\"", "'")), url)
    }
  }.flatten.toSet

  def embeds(html: String, url: Option[String] = None): Set[String] = {
    val handler = embedsHandler(html, url)
    HtmlProcessor.process(html, Set(handler))
    handler.result
  }

  def embedsHandler(html: String, url: Option[String] = None, tags: Set[String] = EmbedTags, filterTag: TagMatch => Boolean = _ => true): TagHandler[Set[String]] = {
    val base = baseUrl(html, url)
    TagHandler.all(Set.empty[String], handleClosing = true) { (tag, _, result) =>
      result ++ {
        if (filterTag(tag)) {
          if (tag.opening) {
            {
              HtmlProcessor.attributeValue(tag.attributes, "background").flatMap(resolveLink(_, base))
            } ++ {
              HtmlProcessor.attributeValue(tag.attributes, "style").toIterator.flatMap(cssEmbeds(_, base))
            } ++ {
              if (EmbedTags.contains(tag.name)) tagTargets(tag, base)
              else Iterator.empty
            }
          } else {
            if (tag.closingTag && tag.name == "style") cssEmbeds(tag.text, base)
            else Iterator.empty
          }
        } else Iterator.empty
      }
    }
  }

  def contentEmbedsHandler(html: String, url: Option[String] = None, minContentTextLength: Int = 30): TagHandler[Set[String]] = {
    var inContent = false
    embedsHandler(html, url, ContentEmbedTags, { tag =>
      if (inContent) true
      else {
        if (tag.closingTag && !HtmlProcessor.CodeTags.contains(tag.name)) inContent = tag.text.trim.length >= minContentTextLength
        inContent
      }
    })
  }
}
