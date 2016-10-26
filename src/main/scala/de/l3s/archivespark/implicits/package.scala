/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark

import java.time.LocalDateTime

import de.l3s.archivespark.enrich.EnrichRoot
import de.l3s.archivespark.implicits.classes._
import de.l3s.archivespark.utils.JsonConvertible
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object implicits {
  implicit class ImplicitEnrichableRDD[Root <: EnrichRoot : ClassTag](rdd: RDD[Root]) extends EnrichableRDD[Root](rdd)
  implicit class ImplicitJsonConvertibleRDD[Record <: JsonConvertible](rdd: RDD[Record]) extends JsonConvertibleRDD[Record](rdd)
  implicit class ImplicitSimplifiedGetterEnrichRoot[Root <: EnrichRoot](root: Root) extends SimplifiedGetterEnrichRoot[Root](root)

  implicit class OrderedLocalDateTime(time: LocalDateTime) extends Ordered[LocalDateTime] {
    override def compare(that: LocalDateTime): Int = time.compareTo(that)
  }
}
