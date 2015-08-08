package de.l3s.archivespark

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD

/**
 * Created by holzmann on 04.08.2015.
 */
abstract class ArchiveRDD(parent: RDD[CdxRecord]) extends RDD[ArchiveRecord](parent) {
  protected abstract def record(from: CdxRecord): ArchiveRecord

  override def compute(split: Partition, context: TaskContext): Iterator[ArchiveRecord] = {
    firstParent[CdxRecord].iterator(split, context).map(r => record(r))
  }

  override protected def getPartitions: Array[Partition] = firstParent.partitions
}