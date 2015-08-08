package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichFunc
import org.apache.spark.rdd.RDD

/**
 * Created by holzmann on 05.08.2015.
 */
object EnrichableRDD {
  implicit class EnrichableRDD[Root <: EnrichRoot[_, Root, _]](rdd: RDD[Root]) {
    def enrich(f: EnrichFunc[Root, _, _]): RDD[Root] = rdd.map(r => r.enrich(f))
  }
}
