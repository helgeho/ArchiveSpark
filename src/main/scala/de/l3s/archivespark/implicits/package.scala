package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichRoot
import de.l3s.archivespark.implicits.classes._
import de.l3s.archivespark.utils.JsonConvertible
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by holzmann on 17.09.2015.
 */
package object implicits extends Implicits {
  implicit class ImplicitEnrichableRDD[Root <: EnrichRoot[_, Root] : ClassTag](rdd: RDD[Root]) extends EnrichableRDD[Root](rdd)
  implicit class ImplicitJsonConvertibleRDD[Record <: JsonConvertible](rdd: RDD[Record]) extends JsonConvertibleRDD[Record](rdd)
  implicit class ImplicitResolvableRDD[Record <: ArchiveRecord : ClassTag](rdd: RDD[Record]) extends ResolvableRDD[Record](rdd)
}
