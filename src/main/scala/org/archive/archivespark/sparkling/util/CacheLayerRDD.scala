package org.archive.archivespark.sparkling.util

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

class CacheLayerRDD(private var prev: RDD[String]) extends RDD[String](prev) {
  override val partitioner: Option[Partitioner] = firstParent[String].partitioner

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val path = firstParent[String].iterator(split, context).next
    val cacheFile = new File(path)
    if (cacheFile.exists) Iterator(path)
    else firstParent[String].compute(split, context)
  }

  override protected def getPartitions: Array[Partition] = firstParent[String].partitions

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
