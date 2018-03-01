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

import java.io._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil

import scala.io.Source

object IOUtil {
  def text(filename: String, printTop: Int = -1)(out: PrintWriter => Unit): Unit = {
    val writer = new PrintWriter(filename)
    out(writer)
    writer.close()
    if (printTop != 0) {
      val source = Source.fromFile(filename)
      val lines = if (printTop < 0) source.getLines else source.getLines.take(printTop)
      lines.foreach(println)
      source.close()
    }
  }

  def appendText(filename: String, printTop: Int = -1)(out: PrintWriter => Unit): Unit = {
    val offset = if (printTop != 0 && exists(filename)) FileUtils.sizeOf(new File(filename)) else 0
    val writer = new PrintWriter(new FileOutputStream(filename, true))
    out(writer)
    writer.close()
    if (printTop != 0) {
      val in = new FileInputStream(filename)
      in.skip(offset)
      val source = Source.fromInputStream(in)
      val lines = if (printTop < 0) source.getLines else source.getLines.take(printTop)
      lines.foreach(println)
      source.close()
    }
  }

  def file(filename: String, append: Boolean = false)(out: OutputStream => Unit): Unit = {
    val stream = new FileOutputStream(filename, append)
    out(stream)
    stream.close()
  }

  def exists(path: String): Boolean = new File(path).exists()

  def existsHdfs(path: String): Boolean = FileSystem.get(SparkHadoopUtil.get.conf).exists(new Path(path))

  def delete(path: String): Unit = FileUtils.deleteQuietly(new File(path))

  def deleteHdfs(path: String): Unit = FileSystem.get(SparkHadoopUtil.get.conf).delete(new Path(path), true)
}
