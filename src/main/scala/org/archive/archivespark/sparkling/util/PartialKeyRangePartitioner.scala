package org.archive.archivespark.sparkling.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, RangePartitioner}

import scala.reflect.ClassTag

class PartialKeyRangePartitioner[K : ClassTag, V, P : ClassTag : Ordering](val numPartitions: Int, rdd: RDD[_ <: (K, V)], partial: K => P, val ascending: Boolean = true) extends Partitioner {
  val rangePartitioner = new RangePartitioner(numPartitions, rdd.map{case (k,v) => (partial(k), true)}, ascending)
  override def getPartition(key: Any): Int = rangePartitioner.getPartition(partial(key.asInstanceOf[K]))
}