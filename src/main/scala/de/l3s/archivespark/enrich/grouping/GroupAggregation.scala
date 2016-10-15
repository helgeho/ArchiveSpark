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

package de.l3s.archivespark.enrich.grouping

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.enrich.{Derivatives, RootEnrichFunc, SingleField}
import de.l3s.archivespark.specific.warc.GroupRecord
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class GroupAggregation[Root, T : ClassTag] private[grouping] (context: GroupContext[Root], field: String, map: Root => T, reduce: (T, T) => T) extends RootEnrichFunc[GroupRecord] with SingleField[T] {
  override def resultField: String = field

  override def prepareGlobal(rdd: RDD[GroupRecord]): RDD[Any] = {
    val keyGroupPairs = rdd.map(r => (r.get, r))
    val keyValuePairs = context.keyRecordPairs.map{case (k, r) => (k, map(r))}.reduceByKey(reduce, ArchiveSpark.parallelism)
    keyGroupPairs.join(keyValuePairs).map{case (k, groupValue) => groupValue}
  }

  private var tmpValue: Option[T] = None
  override def prepareLocal(record: Any): GroupRecord = {
    val (group, valueOpt) = record.asInstanceOf[(GroupRecord, T)]
    tmpValue = Some(valueOpt)
    group
  }

  override def deriveRoot(source: GroupRecord, derivatives: Derivatives): Unit = {
    if (tmpValue.isDefined) derivatives << tmpValue.get
  }
}
