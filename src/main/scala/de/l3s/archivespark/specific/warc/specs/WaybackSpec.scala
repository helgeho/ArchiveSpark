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

package de.l3s.archivespark.specific.warc.specs

import java.net.URLEncoder

import de.l3s.archivespark.dataspecs.DataSpec
import de.l3s.archivespark.specific.warc.{CdxRecord, WaybackRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source

class WaybackSpec private (url: String, matchPrefix: Boolean, from: Long, to: Long, blocksPerPage: Int, pages: Int, maxPartitions: Int) extends DataSpec[String, WaybackRecord] {
  val CdxServerUrl = "http://web.archive.org/cdx/search/cdx?url=$url&matchType=$prefix&pageSize=$blocks&page=$page"

  private def cdxServerUrl(page: Int): String = {
    var uri = CdxServerUrl.replace("$url", URLEncoder.encode(url, "UTF-8"))
    uri = uri.replace("$prefix", if (matchPrefix) "prefix" else "exact")
    uri = uri.replace("$blocks", blocksPerPage.toString)
    uri = uri.replace("$page", page.toString)
    if (from > 0) uri += "&from=" + from
    if (to > 0) uri += "&from=" + to
    uri
  }

  override def load(sc: SparkContext, minPartitions: Int): RDD[String] = {
    sc.parallelize(0 until pages, if (maxPartitions == 0) minPartitions else maxPartitions.min(minPartitions)).flatMap{page =>
      try {
        Source.fromURL(cdxServerUrl(page)).getLines()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Iterator.empty
      }
    }.cache
  }

  override def parse(data: String): Option[WaybackRecord] = {
    CdxRecord.fromString(data).map(cdx => new WaybackRecord(cdx))
  }
}

object WaybackSpec {
  def apply(url: String, matchPrefix: Boolean = false, from: Long = 0, to: Long = 0, blocksPerPage: Int = 5, pages: Int = 50, maxPartitions: Int = 0) = {
    new WaybackSpec(url, matchPrefix, from, to, blocksPerPage, pages, maxPartitions)
  }
}