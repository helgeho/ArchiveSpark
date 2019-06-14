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

import java.net.URL

import org.archive.url.WaybackURLKeyMaker

import scala.util.Try

object SurtUtil {
  private lazy val keyMaker = new WaybackURLKeyMaker()
  private lazy val validHostPattern = "[a-z]+\\,[\\p{L}\\p{M}0-9\\-\\,]+".r

  def fromUrl(url: String): String = Try(keyMaker.makeKey(url)).getOrElse(url)

  def fromUrl(url: String, baseUrl: String): String = {
    val resolved = new URL(new URL(toUrl(baseUrl)), url).toString
    fromUrl(resolved)
  }

  def toUrl(surt: String): String = {
    if (RegexUtil.matchesAbsoluteUrlStart(surt)) return surt
    surt.splitAt(surt.indexOf(')'))
    val (host, path) = surt.splitAt(surt.indexOf(')'))
    val hostSplit = host.split(',')
    "http://" + hostSplit.reverse.mkString(".") + path.drop(1)
  }

  def urlToSurtPrefixes(url: String, subdomains: Boolean = true, urlInSurtFormat: Boolean = false): Set[String] = {
    val surt = if (urlInSurtFormat) url else SurtUtil.fromUrl(if (RegexUtil.matchesAbsoluteUrlStart(url)) url else "http://" + url)
    val hostPath = surt.split("\\)", 2)
    val site = hostPath(0).trim
    if (site.isEmpty) Set.empty
    else hostPath.drop(1).headOption.map(_.trim.stripPrefix("/").stripSuffix("/")).filter(_.nonEmpty) match {
      case Some(path) => Set(site + ")/" + path + " ") ++ (if (path.contains("?")) Set.empty else Set(site + ")/" + path + "/", site + ")/" + path + "?"))
      case None => Set(site + ")") ++ (if (subdomains) Set(site + ",") else Seq.empty)
    }
  }

  def host(surt: String): String = {
    val slash = surt.indexOf('/')
    (if (slash < 0) surt else surt.take(slash)).stripSuffix(")")
  }

  def validateHost(surt: String): Option[String] = {
    Some(host(surt)).filter { host =>
      validHostPattern.pattern.matcher(host).matches && !{
        host.contains("--") || host.contains(",,") || host.split(',').exists(p => p.isEmpty || p.startsWith("-") || p.endsWith("-"))
      }
    }
  }

  def validate(url: String, urlInSurtFormat: Boolean = false): Option[String] = Some(if (urlInSurtFormat) url else fromUrl(url)).filter(validateHost(_).isDefined)
}