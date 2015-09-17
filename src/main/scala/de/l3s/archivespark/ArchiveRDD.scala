package de.l3s.archivespark

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD

/**
 * Created by holzmann on 04.08.2015.
 */
abstract class ArchiveRDD[Base](parent: RDD[Base]) extends RDD[ArchiveRecord](parent) {
  protected def record(from: Base): ArchiveRecord

  override def compute(split: Partition, context: TaskContext): Iterator[ArchiveRecord] = {
    parent.iterator(split, context).filter(r => r != null).map(r => record(r)).filter(r => r != null)
  }

  override protected def getPartitions: Array[Partition] = parent.partitions
}