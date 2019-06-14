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

package org.archive.archivespark.sparkling.util

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.CodingErrorAction

import org.archive.archivespark.sparkling.io.IOUtil

import scala.io.{Codec, Source}
import scala.util.Try

object StringUtil {
  import org.archive.archivespark.sparkling.Sparkling._

  def repeated(str: String, repeat: String => String): String = {
    var prev = str
    var transformed = repeat(str)
    while (transformed != prev) {
      prev = transformed
      transformed = repeat(transformed)
    }
    transformed
  }

  def stripSuffix(str: String, suffix: String, recursive: Boolean = false): String = {
    if (str.toLowerCase.endsWith(suffix.toLowerCase)) str.dropRight(suffix.length)
    else str
  }

  def stripSuffixes(str: String, suffixes: String*): String = {
    var s = str
    for (suffix <- suffixes) s = stripSuffix(s, suffix)
    s
  }

  def stripPrefix(str: String, prefix: String): String = {
    if (str.toLowerCase.startsWith(prefix.toLowerCase)) str.drop(prefix.length)
    else str
  }

  def stripPrefixes(str: String, prefixes: String*): String = {
    var s = str
    for (prefix <- prefixes) s = stripPrefix(s, prefix)
    s
  }

  def stripBracket(str: String, open: String, close: String): String = {
    val lowerStr = str.toLowerCase
    val lowerOpen = open.toLowerCase
    val lowerClose = close.toLowerCase
    if (lowerStr.startsWith(lowerOpen) && lowerStr.endsWith(lowerClose)) str.drop(lowerOpen.length).dropRight(lowerClose.length)
    else str
  }

  def stripBracket(str: String, bracket: String): String = stripBracket(str, bracket, bracket)

  def stripBrackets(str: String, brackets: TraversableOnce[String]): String = stripBrackets(str, brackets.toSeq.map(b => (b,b)): _*)

  def stripBrackets(str: String, brackets: (String, String)*): String = {
    var s = str
    for ((open, close) <- brackets) s = stripBracket(s, open, close)
    s
  }

  def padNum(num: Long, length: Int): String = num.toString.reverse.padTo(length, "0").reverse.mkString

  def filterPrefixes(strings: Set[String], prefixes: Set[String]): TraversableOnce[String] = filterPrefixes(strings.toSeq.sorted, prefixes.toSeq.sorted)

  def filterPrefixes(sortedStrings: TraversableOnce[String], sortedPrefixes: TraversableOnce[String], strict: Boolean = true): Iterator[String] = {
    if (sortedPrefixes.isEmpty) return sortedStrings.toIterator
    val (prefixesOrig, prefixesDup) = if (strict) (sortedPrefixes.toIterator, Iterator.empty) else sortedPrefixes.toIterator.duplicate
    var prefixes = prefixesOrig.buffered
    var backup = prefixesDup
    var prevLine = ""
    sortedStrings.toIterator.filter { line =>
      if (line < prevLine) {
        if (strict) throw new RuntimeException("Strings not in order.")
        else {
          val (prefixesOrig, prefixesDup) = backup.duplicate
          prefixes = prefixesOrig.buffered
          backup = prefixesDup
        }
      }
      prevLine = line
      prefixes.hasNext && (line.startsWith(prefixes.head) || {
        if (line > prefixes.head) IteratorUtil.dropWhile(prefixes)(prefix => line > prefix && !line.startsWith(prefix))
        prefixes.hasNext && line.startsWith(prefixes.head)
      })
    }
  }

  def codec(charset: String, onError: CodingErrorAction = CodingErrorAction.IGNORE): Codec = {
    val codec = Codec(charset)
    codec.onMalformedInput(onError)
    codec.onUnmappableCharacter(onError)
    codec
  }

  def source[R](path: String)(action: Source => R): R = source(path, DefaultCharset)(action)
  def source[R](path: String, charset: String)(action: Source => R): R = source(path, codec(charset))(action)
  def source[R](path: String, codec: Codec)(action: Source => R): R = {
    val source = Source.fromFile(path)(codec)
    val r = action(source)
    source.close()
    r
  }

  def source[R](in: InputStream, charset: String = DefaultCharset)(action: Source => R): R = source(in, codec(charset))(action)
  def source[R](in: InputStream, codec: Codec)(action: Source => R): R = {
    val source = Source.fromInputStream(in)(codec)
    val r = action(source)
    source.close()
    r
  }

  def fromBytes(bytes: Array[Byte], charset: String = DefaultCharset): String = fromBytes(bytes, codec(charset))
  def fromBytes(bytes: Array[Byte], charsets: Seq[String]): String = {
    charsets.toIterator.flatMap { charset =>
      Try(fromBytes(bytes, codec(charset, CodingErrorAction.REPORT))).toOption
    } ++ IteratorUtil.getLazy { _ =>
      fromBytes(bytes, codec(charsets.headOption.getOrElse(DefaultCharset), CodingErrorAction.IGNORE))
    }
  }.next
  def fromBytes(bytes: Array[Byte], codec: Codec): String = source(new ByteArrayInputStream(bytes), codec)(_.mkString)

  def fromInputStream(in: InputStream, charset: String = DefaultCharset): String = fromInputStream(in, codec(charset))
  def fromInputStream(in: InputStream, charsets: Seq[String]): String = fromBytes(IOUtil.bytes(in), charsets)
  def fromInputStream(in: InputStream, codec: Codec): String = source(in, codec)(_.mkString)

  def readLine(in: InputStream, charset: String = DefaultCharset, maxLength: Int = 4096): String = {
    val next = in.read()
    if (next == -1) null
    else {
      val bytes = (Iterator(next.toByte) ++ Iterator.continually(in.read().toByte)).takeWhile(_.toChar != '\n').take(maxLength).toArray
      fromBytes(bytes, charset).stripSuffix("\r")
    }
  }

  def formatNumber[A](number: A, decimal: Int = 0)(implicit numeric: Numeric[A]): String = {
    val decFactor = Math.pow(10, decimal).toLong
    val extended = (numeric.toDouble(number) * decFactor).round
    val int = extended / decFactor
    val dec = extended % decFactor
    val intStr = int.toString.reverse.grouped(3).map(_.reverse.mkString).toList.reverse.mkString(",")
    val decString = if (decimal == 0) "" else "," + dec.toString.reverse.padTo(decimal, " ").reverse.mkString
    intStr + decString
  }
}
