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

package de.l3s.archivespark.specific.books

import com.github.nscala_time.time.Imports._
import de.l3s.archivespark.utils.JsonConvertible

import scala.util.Try
import scala.xml.{Elem, XML}

object BookMetaData {
  val dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def fromMap(meta: Map[String, Seq[String]]): Option[BookMetaData] = {
      Some(BookMetaData(
        meta.getOrElse("title", Seq("")).head,
        meta.getOrElse("creator", Seq("")).head,
        meta.getOrElse("publisher", Seq("")).head,
        meta.getOrElse("date", Seq("")).head,
        meta.getOrElse("language", Seq("")).head,
        meta.getOrElse("publicdate", Seq("")).head,
        meta.getOrElse("subject", Seq()),
        meta.getOrElse("collection", Seq()),
        meta
      ))
  }

  def fromXml(xml: String): Option[BookMetaData] = fromXml(XML.loadString(xml))

  def fromXml(xml: Elem): Option[BookMetaData] = Try {
    val map = xml.child.flatMap { child =>
      if (child.isAtom || child.child.size != 1) Iterator.empty
      else Iterator(child.label -> child.child.head.mkString)
    }.groupBy{case (key, value) => key}.mapValues{case keyValues => keyValues.map{case (key, value) => value}}
    fromMap(map)
  }.getOrElse(None)
}

case class BookMetaData(title: String,
                        creator: String,
                        publisher: String,
                        date: String,
                        language: String,
                        publicdateStr: String,
                        subjects: Seq[String],
                        collections: Seq[String],
                        raw: Map[String, Seq[String]]) extends JsonConvertible {
  def publicdate = Try(BookMetaData.dateTimeFormat.parseDateTime(publicdateStr)).getOrElse(null)

  def toJson: Map[String, Any] = Map[String, Any](
    "title" -> title,
    "creator" -> creator,
    "publisher" -> publisher,
    "date" -> date,
    "language" -> language,
    "publicdate" -> publicdateStr,
    "subjects" -> subjects.mkString(", "),
    "collections" -> collections.mkString(", ")
  )
}
