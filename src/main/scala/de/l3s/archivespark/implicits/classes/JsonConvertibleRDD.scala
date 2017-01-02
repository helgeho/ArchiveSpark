/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2017 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.implicits.classes

import de.l3s.archivespark.utils.JsonConvertible
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

class JsonConvertibleRDD[Record <: JsonConvertible](rdd: RDD[Record]) {
  def toJson = rdd.map(r => r.toJson)

  def toJsonStrings = rdd.map(r => r.toJsonString)
  def toJsonStrings(pretty: Boolean = true) = rdd.map(r => r.toJsonString(pretty))

  def saveAsJson(path: String) = if (path.endsWith(".gz")) toJsonStrings.saveAsTextFile(path, classOf[GzipCodec]) else toJsonStrings.saveAsTextFile(path)

  def saveToEs(resource: String) = EsSpark.saveJsonToEs(rdd.map(r => r.toJsonString(pretty = false)), resource)

  def peekJson = rdd.take(1).head.toJsonString
}
