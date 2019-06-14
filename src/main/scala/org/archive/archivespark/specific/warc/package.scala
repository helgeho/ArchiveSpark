package org.archive.archivespark.specific

import org.apache.spark.rdd.RDD
import org.archive.archivespark.sparkling.cdx.CdxRecord
import org.archive.archivespark.specific.warc.implicits.{CdxRDD, WarcRDD}

import scala.reflect.ClassTag

package object warc {
  implicit class ImplicitResolvableRDD(rdd: RDD[CdxRecord]) extends CdxRDD(rdd)
  implicit class ImplicitWarcRDD[WARC <: WarcLikeRecord : ClassTag](rdd: RDD[WARC]) extends WarcRDD(rdd)
}
