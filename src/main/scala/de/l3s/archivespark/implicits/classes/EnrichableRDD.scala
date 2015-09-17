package de.l3s.archivespark.implicits.classes

import de.l3s.archivespark.enrich.{EnrichRoot, EnrichFunc}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by holzmann on 05.08.2015.
 */
class EnrichableRDD[Root <: EnrichRoot[_, Root] : ClassTag](rdd: RDD[Root]) {
  def enrich(f: EnrichFunc[Root, _]): RDD[Root] = rdd.map(r => f.enrich(r))
}
