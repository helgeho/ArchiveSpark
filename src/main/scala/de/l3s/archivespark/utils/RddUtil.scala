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

package de.l3s.archivespark.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.reflect.ClassTag

object RddUtil {
  class ItemParitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key match {
      case k: Int => k % numPartitions
      case _ => key.hashCode % numPartitions
    }
  }

  def repartition[T : ClassTag](sc: SparkContext, rdd: RDD[T], numPartitions: Int): RDD[T] = {
    val partitioner = new ItemParitioner(numPartitions)
    rdd.zipWithIndex.map{case (v, i) => (i, v)}.partitionBy(partitioner).values
  }

  def parallelize(sc: SparkContext, items: Int): RDD[Int] = parallelize(sc, items, items)

  def parallelize(sc: SparkContext, items: Int, partitions: Int): RDD[Int] = parallelize(sc, 0 until items, partitions)

  def parallelize[T : ClassTag](sc: SparkContext, items: Seq[T]): RDD[T] = parallelize(sc, items, items.size)

  def parallelize[T : ClassTag](sc: SparkContext, items: Seq[T], partitions: Int): RDD[T] = {
    repartition(sc, sc.parallelize(items), partitions.min(items.size))
  }
}
