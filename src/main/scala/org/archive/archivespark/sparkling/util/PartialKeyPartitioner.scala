package org.archive.archivespark.sparkling.util

import org.apache.spark.{HashPartitioner, Partitioner}

import scala.reflect.ClassTag

class PartialKeyPartitioner[K : ClassTag, P : ClassTag](val numPartitions: Int, partial: K => P) extends Partitioner {
  val hashPartitioner = new HashPartitioner(numPartitions)
  override def getPartition(key: Any): Int = hashPartitioner.getPartition(partial(key.asInstanceOf[K]))
}