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

package de.l3s.archivespark.enrich.grouping

import de.l3s.archivespark.enrich.{DefaultFieldAccess, EnrichFunc, EnrichRoot}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class GroupContext[Root <: EnrichRoot] private[archivespark] (private[grouping] val keyRecordPairs: RDD[(Map[String, Any], Root)]) {
  def aggregate[T : ClassTag](target: String, map: Root => Option[T])(reduce: (T, T) => T): GroupAggregation[Root, T] = {
    new GroupAggregation[Root, T](this, target, map, reduce)
  }

  def aggregateValues[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](target: String, func: EnrichFunc[SpecificRoot, _], field: String)(reduce: (T, T) => T): GroupAggregation[Root, T] = {
    new GroupAggregation[Root, T](this, target, func.enrich(_, excludeFromOutput = true).get(func.pathTo(field)), reduce)
  }

  def aggregateValues[SpecificRoot >: Root <: EnrichRoot, T : ClassTag](target: String, func: EnrichFunc[SpecificRoot, _] with DefaultFieldAccess[T])(reduce: (T, T) => T): GroupAggregation[Root, T] = {
    aggregateValues[SpecificRoot, T](target, func, func.defaultField)(reduce)
  }
}