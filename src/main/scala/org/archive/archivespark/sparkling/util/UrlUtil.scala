package org.archive.archivespark.sparkling.util

import java.net.{URLDecoder, URLEncoder}

import org.apache.commons.lang.StringEscapeUtils

object UrlUtil {
  def decode(url: String, html: Boolean = true): String = {
    val encoded = if (html) StringEscapeUtils.unescapeHtml(url) else url
    URLDecoder.decode(encoded, "UTF-8")
  }

  def encode(url: String): String = URLEncoder.encode(url, "UTF-8")
}
