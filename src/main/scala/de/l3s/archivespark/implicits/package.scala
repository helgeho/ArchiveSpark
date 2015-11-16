/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 */

package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichRoot
import de.l3s.archivespark.implicits.classes._
import de.l3s.archivespark.utils.JsonConvertible
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object implicits extends Implicits {
  implicit class ImplicitEnrichableRDD[Root <: EnrichRoot[_] : ClassTag](rdd: RDD[Root]) extends EnrichableRDD[Root](rdd)
  implicit class ImplicitJsonConvertibleRDD[Record <: JsonConvertible](rdd: RDD[Record]) extends JsonConvertibleRDD[Record](rdd)
  implicit class ImplicitResolvableRDD[Record <: ArchiveRecord : ClassTag](rdd: RDD[Record]) extends ResolvableRDD[Record](rdd)
}
