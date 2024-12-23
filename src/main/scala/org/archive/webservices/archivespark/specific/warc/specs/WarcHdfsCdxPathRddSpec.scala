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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.cdx.CdxRecord
import org.archive.webservices.archivespark.specific.warc.WarcRecord

class WarcHdfsCdxPathRddSpec private(cdx: RDD[(CdxRecord, String)]) extends WarcHdfsCdxSpecBase[(CdxRecord, String)] {
  override def load(sc: SparkContext, minPartitions: Int): RDD[(CdxRecord, String)] = cdx

  override def parse(cdxPath: (CdxRecord, String)): Option[WarcRecord] = {
    val (cdx, dir) = cdxPath
    parse(cdx, new Path(dir, cdx.locationFromAdditionalFields._1))
  }
}

object WarcHdfsCdxPathRddSpec {
  def apply(cdxWarcPaths: RDD[(CdxRecord, String)]) = new WarcHdfsCdxPathRddSpec(cdxWarcPaths)
}