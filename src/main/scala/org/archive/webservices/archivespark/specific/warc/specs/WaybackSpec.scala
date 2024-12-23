/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2024 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.specific.warc.specs

import java.net.URLEncoder

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.archive.webservices.archivespark.dataspecs.DataSpec
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.cdx.CdxRecord
import org.archive.webservices.sparkling.util.{IteratorUtil, RddUtil, StringUtil}
import org.archive.webservices.archivespark.specific.warc.WaybackRecord

import scala.io.Source

class WaybackSpec (cdxServerUrl: String, pages: Int, maxPartitions: Int) extends DataSpec[String, WaybackRecord] {
  override def load(sc: SparkContext, minPartitions: Int): RDD[String] = {
    if (pages == 1) {
      val source = Source.fromURL(cdxServerUrl)(StringUtil.codec(Sparkling.DefaultCharset))
      try {
        val lines = source.getLines.toList
        RddUtil.parallelize(lines, if (maxPartitions == 0) minPartitions else maxPartitions.min(minPartitions))
      } finally {
        source.close()
      }
    } else {
      RddUtil.parallelize(pages, if (maxPartitions == 0) minPartitions else maxPartitions.min(minPartitions)).flatMap { page =>
        try {
          val source = Source.fromURL(cdxServerUrl + "&page=" + page)(StringUtil.codec(Sparkling.DefaultCharset))
          IteratorUtil.cleanup(source.getLines, source.close)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            Iterator.empty
        }
      }.cache
    }
  }

  override def parse(data: String): Option[WaybackRecord] = CdxRecord.fromString(data).map(cdx => new WaybackRecord(cdx))
}

object WaybackSpec {
  def apply(url: String, matchPrefix: Boolean = false, from: Long = 0, to: Long = 0, blocksPerPage: Int = 5, pages: Int = 50, maxPartitions: Int = 0): WaybackSpec = {
    var cdxServerUrl = "http://web.archive.org/cdx/search/cdx?url=$url&matchType=$prefix&pageSize=$blocks"
    cdxServerUrl = cdxServerUrl.replace("$url", URLEncoder.encode(url, "UTF-8"))
    cdxServerUrl = cdxServerUrl.replace("$prefix", if (matchPrefix) "prefix" else "exact")
    cdxServerUrl = cdxServerUrl.replace("$blocks", blocksPerPage.toString)
    if (from > 0) cdxServerUrl += "&from=" + from
    if (to > 0) cdxServerUrl += "&to=" + to
    new WaybackSpec(cdxServerUrl, pages, maxPartitions)
  }
}