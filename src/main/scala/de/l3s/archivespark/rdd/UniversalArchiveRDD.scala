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

package de.l3s.archivespark.rdd

import de.l3s.archivespark.cdx.CdxRecord
import de.l3s.archivespark.records.UniversalArchiveRecord
import de.l3s.archivespark.{ArchiveRDD, ArchiveSpark}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object UniversalArchiveRDD {
  def apply(cdxPath: String)(implicit sc: SparkContext) = new UniversalArchiveRDD(ArchiveSpark.cdx(cdxPath))
}

class UniversalArchiveRDD private (parent: RDD[CdxRecord]) extends ArchiveRDD[CdxRecord](parent) {
  override protected def record(cdx: CdxRecord) = new UniversalArchiveRecord(cdx)
}