package org.archive.archivespark.sparkling.util

import scala.util.matching.Regex

object RegexUtil {
  private val patternCache = CollectionUtil.concurrentMap[String, Regex]

  def r(pattern: String): Regex = patternCache.getOrElseUpdate(pattern, pattern.r)

  lazy val urlStartPattern: Regex = r("[a-z]+\\:.*")
  lazy val noWordCharacterPattern: Regex = r("[^\\p{L}\\p{M}]+")
  lazy val newLineSpaceTabPattern: Regex = r("""[\n\r\t ]+""")
  lazy val tokenDelimiterPattern: Regex = r("[^\\p{L}\\p{M}0-9]+")

  def matchesAbsoluteUrlStart(url: String): Boolean = urlStartPattern.pattern.matcher(url.toLowerCase).matches
  def oneValidWordCharacter(str: String): Boolean = str.nonEmpty && !noWordCharacterPattern.pattern.matcher(str).matches
  def oneLineSpaceTrim(str: String): String = newLineSpaceTabPattern.replaceAllIn(str, " ").trim
  def tokenize(str: String): Array[String] = tokenDelimiterPattern.replaceAllIn(str.toLowerCase, " ").trim.split(' ')

  def split(str: String, pattern: String, limit: Int = -1): Seq[String] = {
    if (str.isEmpty) return Seq(str)
    if (limit < 1) r(pattern).split(str).toSeq else {
      val p = r(pattern)
      var remaining = str
      var count = 1
      var eof = false
      IteratorUtil.whileDefined {
        if (eof) None
        else {
          if (count == limit) {
            eof = true
            Some(remaining)
          } else if (remaining.nonEmpty) Some {
            p.findFirstMatchIn(remaining) match {
              case Some(m) =>
                count += 1
                remaining = m.after.toString
                m.before.toString
              case None =>
                eof = true
                remaining
            }
          } else None
        }
      }.toList
    }
  }
}
