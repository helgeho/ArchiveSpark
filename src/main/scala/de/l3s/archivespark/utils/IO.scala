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

package de.l3s.archivespark.utils

import java.io.{FileInputStream, File, FileFilter}
import java.util.zip.GZIPInputStream

import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.hadoop.fs.Path

import scala.io.Source
import scala.util.Try

object IO {
  def lines(globPath: String): Seq[String] = {
    val path = new Path(globPath)
    val fileFilter: FileFilter = new WildcardFileFilter(path.getName)
    new File(path.getParent.toString).listFiles(fileFilter).flatMap{file =>
      var fileStream: FileInputStream = null
      var gzipStream: GZIPInputStream = null
      var source: Source = null
      try {
        source = if (file.getName.toLowerCase.endsWith(".gz")) {
          fileStream = new FileInputStream(file)
          gzipStream = new GZIPInputStream(fileStream)
          Source.fromInputStream(gzipStream)
        } else Source.fromFile(file)
        source.getLines().toList
      } finally {
        if (gzipStream != null) Try {gzipStream.close()}
        if (fileStream != null) Try {fileStream.close()}
        if (source != null) Try {source.close()}
      }
    }.toSeq
  }
}
