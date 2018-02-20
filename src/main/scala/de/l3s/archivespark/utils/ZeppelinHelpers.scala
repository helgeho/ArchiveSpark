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

import org.apache.spark.rdd.RDD

import scala.collection.immutable

object ZeppelinHelpers {
  def table(rdd: RDD[Seq[Any]], cols: String*): String = {
    val table = rdd.map(seq => seq.map(_.toString).mkString("\t")).collect.mkString("\n")
    "%table " + cols.mkString("\t") + "\n" + table
  }

  def table(values: collection.Map[_, _], keyCol: String, valCol: String): String = {
    val table = values.toSeq.map{case (k, v) => s"$k\t$v"}.mkString("\n")
    s"%table $keyCol\t$valCol\n$table"
  }

  def table(values: immutable.Map[_, _], keyCol: String, valCol: String): String = table(values.toMap, keyCol, valCol)
}
