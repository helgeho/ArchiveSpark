package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichFunc
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by holzmann on 05.08.2015.
 */
object EnrichableRDD {
  implicit class EnrichableRDD[Root <: EnrichRoot[_, Root] : ClassTag](rdd: RDD[Root]) {
    def enrich(f: EnrichFunc[Root, _]): RDD[Root] = rdd.map(r => r.enrich(f))
  }
}
