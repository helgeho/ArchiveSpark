package org.archive.archivespark.sparkling.util

import org.apache.spark.{HashPartitioner, Partitioner}

import scala.util.Try

class PrimaryKeyPartitioner (val numPartitions: Int) extends Partitioner {
  val hashPartitioner = new HashPartitioner(numPartitions)
  override def getPartition(key: Any): Int = hashPartitioner.getPartition(Try(key.asInstanceOf[Tuple2[_,_]]._1).getOrElse(key))
}