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

package de.l3s.archivespark.utils

import java.io.OutputStream

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD

object SparkIO {
  def defaultCreate(fs: FileSystem, path: Path): OutputStream = fs.create(path, true)

  def save[T](dir: String, rdd: RDD[T], create: (FileSystem, Path) => OutputStream = defaultCreate)(savePartition: (Int, Iterator[T], String => (=> OutputStream => Unit) => Unit) => Long): Long = {
    val fs = FileSystem.newInstance(SparkHadoopUtil.get.conf)
    val dirPath = new Path(dir)
    if (fs.exists(dirPath)) throw new AlreadyExistsException(s"Output directory already exists ($dir).")
    fs.mkdirs(dirPath)

    rdd.mapPartitionsWithIndex{case (idx, records) =>
      def open(filename: String)(action: => OutputStream => Unit): Unit = {
        val fs = FileSystem.get(SparkHadoopUtil.get.conf)
        var stream: OutputStream = null
        try {
          lazy val lazyStream = {
            stream = create(fs, new Path(dir, filename))
            stream
          }
          action(lazyStream)
        } finally {
          if (stream != null) stream.close()
        }
      }
      val processed = savePartition(idx, records, open)
      Iterator(processed)
    }.reduce(_ + _)
  }
}
