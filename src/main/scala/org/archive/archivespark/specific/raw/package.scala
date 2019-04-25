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

package org.archive.archivespark.specific

import java.io.PrintStream
import java.util.zip.GZIPOutputStream

import org.archive.archivespark.utils.SparkIO
import org.apache.spark.rdd.RDD

package object raw {
  implicit class RawTextRDD(rdd: RDD[String]) {
    def saveAsRawText(path: String): Long = {
      val gz = path.toLowerCase.endsWith(".gz")
      SparkIO.save(path, rdd) { (idx, records, open) =>
        val id = idx.toString.reverse.padTo(5, '0').reverse.mkString
        val filename = s"part-$id${if (gz) ".gz" else ""}"

        var processed = 0L
        if (records.nonEmpty) {
          open(filename) { stream =>
            val compressed = if (gz) Some(new GZIPOutputStream(stream)) else None
            val out = new PrintStream(compressed.getOrElse(stream))

            processed = records.map { record =>
              out.println(record)
              1L
            }.sum

            if (compressed.isDefined) compressed.get.finish()
          }
        }
        processed
      }
    }
  }
}
