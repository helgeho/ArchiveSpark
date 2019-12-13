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

package org.archive.archivespark.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil

import scala.util.Try

case class FilePathMap(path: String, patterns: Seq[String] = Seq.empty) {
  val pathMap: Map[String, String] = {
    var map = collection.mutable.Map[String, String]()

    val p = new Path(path)
    val fs = p.getFileSystem(SparkHadoopUtil.get.conf)
    val files = fs.listFiles(p, true)
    while (files.hasNext) {
      val path = files.next.getPath
      val filename = path.getName
      if (patterns.isEmpty || patterns.exists(filename.matches)) {
        if (map.contains(filename)) throw new RuntimeException("duplicate filename: " + filename + " in path: " + path)
        map += filename -> path.getParent.toString.intern
      }
    }

    map.toMap
  }

  def pathToFile(file: String): Option[Path] = Try {new Path(file).getName}.toOption match {
    case Some(f) => pathMap.get(f).map(dir => new Path(dir, f))
    case None => None
  }
}
