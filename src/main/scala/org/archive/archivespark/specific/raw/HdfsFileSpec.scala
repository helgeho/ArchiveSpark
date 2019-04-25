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

package org.archive.archivespark.specific.raw

import org.archive.archivespark.dataspecs.DataSpec
import org.archive.archivespark.dataspecs.access.HdfsFileAccessor
import org.archive.archivespark.utils.{FilePathMap, RddUtil}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD

class HdfsFileSpec private(path: String, filePatterns: Seq[String], decompress: Boolean, maxPartitions: Int, retryDelayMs: Option[Int]) extends DataSpec[String, FileStreamRecord] {
  override def load(sc: SparkContext, minPartitions: Int): RDD[String] = {
    val fs = FileSystem.get(SparkHadoopUtil.get.conf)
    val files = fs.listFiles(new Path(path), true)
    var paths = collection.mutable.Seq.empty[String]
    while (files.hasNext) {
      val path = files.next.getPath
      if (filePatterns.isEmpty || filePatterns.exists(path.getName.matches)) {
        paths :+= path.toString
      }
    }
    RddUtil.parallelize(sc, paths, if (maxPartitions == 0) minPartitions else maxPartitions.min(minPartitions))
  }

  override def parse(data: String): Option[FileStreamRecord] = {
    Some(new FileStreamRecord(data, new HdfsFileAccessor(data, decompress), retryDelayMs))
  }
}

object HdfsFileSpec {
  val DefaultRetryDelay: Int = 1000

  def apply(path: String, filePatterns: Seq[String] = Seq.empty, decompress: Boolean = true, maxPartitions: Int = 0, retryDelayMs: Option[Int] = Some(DefaultRetryDelay)): HdfsFileSpec = {
    new HdfsFileSpec(path, filePatterns, decompress, maxPartitions, retryDelayMs)
  }
}