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

package org.archive.webservices.archivespark.specific.warc

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.cdx.CdxRecord
import org.archive.webservices.archivespark.specific.warc.specs._

object WarcSpec {
  def fromWaybackWithLocalCdx(cdxPath: String): WaybackCdxHdfsSpec = WaybackCdxHdfsSpec(cdxPath)
  def fromWayback(url: String, matchPrefix: Boolean = false, from: Long = 0, to: Long = 0, blocksPerPage: Int = 5, pages: Int = 50, maxPartitions: Int = 0): WaybackSpec = {
    WaybackSpec(url, matchPrefix, from, to, blocksPerPage, pages, maxPartitions)
  }
  def fromWaybackByCdxQuery(cdxServerUrl: String, pages: Int = 50, maxPartitions: Int = 0): WaybackSpec = {
    new WaybackSpec(cdxServerUrl, pages, maxPartitions)
  }
  def fromFiles(path: String, includeRevisits: Boolean = true, includeOthers: Boolean = false): WarcHdfsSpec = WarcHdfsSpec(path, includeRevisits, includeOthers)
  def fromFiles(cdx: RDD[CdxRecord], warcPath: String): WarcHdfsCdxRddSpec = WarcHdfsCdxRddSpec(cdx, warcPath)
  def fromFiles(cdxWarcPaths: RDD[(CdxRecord, String)]): WarcHdfsCdxPathRddSpec = WarcHdfsCdxPathRddSpec(cdxWarcPaths)
  def fromFiles(cdxPath: String, warcPath: String): WarcCdxHdfsSpec = WarcCdxHdfsSpec(cdxPath, warcPath)
  def fromFilesWithCdx(path: String): WarcCdxHdfsSpec = WarcCdxHdfsSpec(path)
}
