package org.archive.archivespark.sparkling.util

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.CodingErrorAction

import org.archive.archivespark.sparkling.Sparkling
import org.archive.archivespark.sparkling.io.IOUtil

import scala.io.{Codec, Source}
import scala.util.Try

object StringUtil {
  import Sparkling._

  def repeated(str: String, repeat: String => String): String = {
    var prev = str
    var transformed = repeat(str)
    while (transformed != prev) {
      prev = transformed
      transformed = repeat(transformed)
    }
    transformed
  }

  def stripPrefixBySeparator(str: String, separator: String): String = {
    val idx = str.indexOf(separator)
    if (idx < 0) str else str.drop(idx + separator.length)
  }

  def prefixBySeparator(str: String, separator: String): String = {
    val idx = str.indexOf(separator)
    if (idx < 0) str else str.take(idx)
  }

  def prependPrefix(str: String, prependSeparator: String, prependLength: Int, prefixSeparator: String): String = stripPrefixBySeparator(str, prependSeparator).take(prependLength) +
    prefixBySeparator(str, prefixSeparator)

  def prepend(str: String, prependWith: String => String, separator: String): String = prependWith(str) + separator + str.split(' ')

  def stripSuffix(str: String, suffix: String, recursive: Boolean = false): String = { if (str.toLowerCase.endsWith(suffix.toLowerCase)) str.dropRight(suffix.length) else str }

  def stripSuffixes(str: String, suffixes: String*): String = {
    var s = str
    for (suffix <- suffixes) s = stripSuffix(s, suffix)
    s
  }

  def stripPrefix(str: String, prefix: String): String = { if (str.toLowerCase.startsWith(prefix.toLowerCase)) str.drop(prefix.length) else str }

  def stripPrefixes(str: String, prefixes: String*): String = {
    var s = str
    for (prefix <- prefixes) s = stripPrefix(s, prefix)
    s
  }

  def stripBracket(str: String, open: String, close: String): String = {
    val lowerStr = str.toLowerCase
    val lowerOpen = open.toLowerCase
    val lowerClose = close.toLowerCase
    if (lowerStr.startsWith(lowerOpen) && lowerStr.endsWith(lowerClose)) str.drop(lowerOpen.length).dropRight(lowerClose.length) else str
  }

  def stripBracket(str: String, bracket: String): String = stripBracket(str, bracket, bracket)

  def stripBrackets(str: String, brackets: TraversableOnce[String]): String = stripBrackets(str, brackets.toSeq.map(b => (b, b)): _*)

  def stripBrackets(str: String, brackets: (String, String)*): String = {
    var s = str
    for ((open, close) <- brackets) s = stripBracket(s, open, close)
    s
  }

  def padRight(str: String, length: Int, char: Character): String = str.padTo(length, char).mkString
  def padLeft(str: String, length: Int, char: Character): String = str.reverse.padTo(length, char).reverse.mkString

  def padNum(num: Long, length: Int): String = padLeft(num.toString, length, '0')

  def filterPrefixes(strings: Set[String], prefixes: Set[String]): TraversableOnce[String] = filterPrefixes(strings.toSeq.sorted, prefixes.toSeq.sorted.toIterator.buffered)

  def filterPrefixes(sortedStrings: TraversableOnce[String], sortedPrefixes: BufferedIterator[String], strict: Boolean = true): Iterator[String] = matchPrefixes(sortedStrings, sortedPrefixes, strict)
    .map(_._1)

  def matchPrefixes(sortedStrings: TraversableOnce[String], sortedPrefixes: BufferedIterator[String], strict: Boolean = true): Iterator[(String, String)] = {
    if (sortedPrefixes.isEmpty || sortedStrings.isEmpty) return Iterator.empty
    var prevLine = ""
    IteratorUtil.zipNext(sortedStrings.toIterator).flatMap { case (line, next) =>
      val outlier = line < prevLine || next.exists(n => line > n && prevLine <= n)
      if (outlier) {
        if (strict) throw new RuntimeException("Strings not in order.")
        None
      } else {
        prevLine = line
        if (
          sortedPrefixes.hasNext &&
          (line.startsWith(sortedPrefixes.head) || {
            if (line > sortedPrefixes.head) IteratorUtil.dropWhile(sortedPrefixes)(prefix => prefix < line && !line.startsWith(prefix))
            sortedPrefixes.hasNext && line.startsWith(sortedPrefixes.head)
          })
        ) Some((line, sortedPrefixes.head))
        else None
      }
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
    try {
      val r = action(source)
      r
    } finally { source.close() }
  }

  def source[R](in: InputStream, charset: String = DefaultCharset)(action: Source => R): R = source(in, codec(charset))(action)
  def source[R](in: InputStream, codec: Codec)(action: Source => R): R = {
    val source = Source.fromInputStream(in)(codec)
    try {
      val r = action(source)
      r
    } finally { source.close() }
  }

  def fromBytes(bytes: Array[Byte], charset: String = DefaultCharset): String = fromBytes(bytes, codec(charset))
  def fromBytes(bytes: Array[Byte], charsets: Seq[String]): String = {
    charsets.toIterator.flatMap { charset => Try(fromBytes(bytes, codec(charset, CodingErrorAction.REPORT))).toOption } ++ IteratorUtil.getLazy { _ =>
      fromBytes(bytes, codec(charsets.headOption.getOrElse(DefaultCharset), CodingErrorAction.IGNORE))
    }
  }.next
  def fromBytes(bytes: Array[Byte], codec: Codec): String = source(new ByteArrayInputStream(bytes), codec)(_.mkString)

  def fromInputStream(in: InputStream, charset: String = DefaultCharset): String = fromInputStream(in, codec(charset))
  def fromInputStream(in: InputStream, charsets: Seq[String]): String = fromBytes(IOUtil.bytes(in), charsets)
  def fromInputStream(in: InputStream, codec: Codec): String = source(in, codec)(_.mkString)

  def readLine(in: InputStream, charset: String = DefaultCharset, maxLength: Int = 4096 * 1024): String = { // 4 MB
    val head = in.read()
    if (head == -1) return null
    val bytes = (Iterator(head) ++ Iterator.continually(in.read())).takeWhile(_ != -1).map(_.toByte).takeWhile(_.toChar != '\n')
    val line = (if (maxLength < 0) bytes else bytes.take(maxLength)).toArray
    fromBytes(line, charset).stripSuffix("\r")
  }

  def formatNumber[A](number: A, decimal: Int = 0)(implicit numeric: Numeric[A]): String = {
    val decFactor = Math.pow(10, decimal).toLong
    val extended = (numeric.toDouble(number) * decFactor).round
    val int = extended / decFactor
    val dec = extended % decFactor
    val intStr = int.toString.reverse.grouped(3).map(_.reverse.mkString).toList.reverse.mkString(",")
    val decString = if (decimal == 0) "" else "." + dec
    intStr + decString
  }
}
