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

package de.l3s.archivespark.enrich.grouping

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.enrich._
import org.apache.spark.rdd.RDD

import scala.annotation.meta.param
import scala.reflect.ClassTag

class GroupAggregation[Root <: EnrichRoot, T : ClassTag] private[grouping] (@(transient @param) context: GroupContext[Root], field: String, map: Root => Option[T], reduce: (T, T) => T) extends RootEnrichFunc[GroupRecord] with EnrichFuncWithDefaultField[GroupRecord, Any, T, T] with SingleField[T] {
  override def resultField: String = field

  override def prepareGlobal(rdd: RDD[GroupRecord]): RDD[Any] = {
    val partitions = ArchiveSpark.partitions(rdd.context)
    val keyGroupPairs = rdd.map(r => (r.get, r))
    val keyValuePairs = keyGroupPairs.map{case (k, g) => (k, true)}.join(context.keyRecordPairs).flatMap{case (k, (_, record)) =>
      map(record).map(v => (k,v))
    }.reduceByKey(reduce, partitions)
    keyGroupPairs.leftOuterJoin(keyValuePairs).map{case (k, groupValueOpt) => groupValueOpt}
  }

  private var tmpValue: Option[T] = None
  override def prepareLocal(record: Any): GroupRecord = {
    val (group, valueOpt) = record.asInstanceOf[(GroupRecord, Option[T])]
    tmpValue = valueOpt
    group
  }

  override def deriveRoot(source: GroupRecord, derivatives: Derivatives): Unit = {
    if (tmpValue.isDefined) derivatives << tmpValue.get
  }
}
