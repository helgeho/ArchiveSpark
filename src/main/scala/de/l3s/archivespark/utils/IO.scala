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

import java.io.{File, FileFilter, FileInputStream}
import java.util.zip.GZIPInputStream

import org.apache.commons.io.filefilter.WildcardFileFilter

import scala.io.Source
import scala.util.Try

object IO {
  def lines(globPath: String): Seq[String] = {
    IO.paths(globPath).flatMap{file =>
      var fileStream: FileInputStream = null
      var gzipStream: GZIPInputStream = null
      var source: Source = null
      try {
        source = if (file.toLowerCase.endsWith(".gz")) {
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
    }
  }

  def lazyLines(globPath: String): Seq[String] = {
    IO.paths(globPath).view.flatMap{file =>
      var fileStream: FileInputStream = null
      var gzipStream: GZIPInputStream = null
      var source: Source = null
      try {
        source = if (file.toLowerCase.endsWith(".gz")) {
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
    }
  }

  def paths(globPath: String): Seq[String] = {
    val delimiter = globPath.replace('\\', '/').lastIndexOf('/')
    val (dir, filename) = if (delimiter < 0) (".", globPath)
    else if (delimiter == 0) (globPath.head.toString, globPath.substring(1))
    else (globPath.substring(0, delimiter), globPath.substring(delimiter + 1))
    val fileFilter: FileFilter = new WildcardFileFilter(filename)
    new File(dir).listFiles(fileFilter).map(f => f.toString).toSeq
  }
}
