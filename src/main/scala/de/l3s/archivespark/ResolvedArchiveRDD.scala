package de.l3s.archivespark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

/**
 * Created by holzmann on 04.08.2015.
 */
abstract class ResolvedArchiveRDD(parent: RDD[CdxRecord]) extends RDD[ResolvedArchiveRecord](parent) {
  protected def record(from: CdxRecord): ResolvedArchiveRecord

  override def compute(split: Partition, context: TaskContext): Iterator[ResolvedArchiveRecord] = {
    firstParent[CdxRecord].iterator(split, context).map(r => record(r)).filter(r => r != null)
  }

  override protected def getPartitions: Array[Partition] = firstParent.partitions
}