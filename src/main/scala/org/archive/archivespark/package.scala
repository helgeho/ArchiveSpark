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

package org.archive

import java.time.LocalDateTime

import org.apache.spark.rdd.RDD
import org.archive.archivespark.implicits.{EnrichableRDD, GenericHelpersRDD, JsonConvertibleRDD, SimplifiedGetterEnrichRoot, StringRDD}
import org.archive.archivespark.model.EnrichRoot
import org.archive.archivespark.util.JsonConvertible

import scala.reflect.ClassTag

package object archivespark {
  implicit class ImplicitStringRDD(rdd: RDD[String]) extends StringRDD(rdd)
  implicit class ImplicitGenericHelpersRDD[A : ClassTag](rdd: RDD[A]) extends GenericHelpersRDD[A](rdd)
  implicit class ImplicitEnrichableRDD[Root <: EnrichRoot : ClassTag](rdd: RDD[Root]) extends EnrichableRDD[Root](rdd)
  implicit class ImplicitJsonConvertibleRDD[Record <: JsonConvertible : ClassTag](rdd: RDD[Record]) extends JsonConvertibleRDD[Record](rdd)
  implicit class ImplicitSimplifiedGetterEnrichRoot[Root <: EnrichRoot](root: Root) extends SimplifiedGetterEnrichRoot[Root](root)

  implicit class OrderedLocalDateTime(time: LocalDateTime) extends Ordered[LocalDateTime] {
    override def compare(that: LocalDateTime): Int = time.compareTo(that)
  }
}
