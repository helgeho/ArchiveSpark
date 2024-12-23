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

package org.archive.webservices.archivespark.specific.raw

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.archive.webservices.archivespark.dataspecs.DataSpec
import org.archive.webservices.archivespark.dataspecs.access.HdfsFileAccessor
import org.archive.webservices.sparkling.io.HdfsIO
import org.archive.webservices.sparkling.util.RddUtil

class HdfsFileSpec private(path: String, filePatterns: Seq[String], decompress: Boolean, maxPartitions: Int) extends DataSpec[String, FileStreamRecord] {
  override def load(sc: SparkContext, minPartitions: Int): RDD[String] = {
    val files = HdfsIO.files(path, recursive = true)
    val filtered = if (filePatterns.isEmpty) files.toSeq else files.filter(path => filePatterns.exists(new Path(path).getName.matches)).toSeq
    RddUtil.parallelize(filtered, if (maxPartitions == 0) minPartitions else maxPartitions.min(minPartitions))
  }

  override def parse(file: String): Option[FileStreamRecord] = Some(new FileStreamRecord(file, new HdfsFileAccessor(file, decompress)))
}

object HdfsFileSpec {
  def apply(path: String, filePatterns: Seq[String] = Seq.empty, decompress: Boolean = true, maxPartitions: Int = 0): HdfsFileSpec = {
    new HdfsFileSpec(path, filePatterns, decompress, maxPartitions)
  }
}