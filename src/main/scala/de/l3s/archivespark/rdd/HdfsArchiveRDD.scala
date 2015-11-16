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

import de.l3s.archivespark.cdx.ResolvedCdxRecord
import de.l3s.archivespark.records.ResolvedHdfsArchiveRecord
import de.l3s.archivespark.ArchiveSpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HdfsArchiveRDD {
  def apply(cdxPath: String, warcPath: String)(implicit sc: SparkContext): HdfsArchiveRDD = {
    new HdfsArchiveRDD(warcPath, ArchiveSpark.resolvedCdxWithPath(cdxPath, warcPath))
  }
}

class HdfsArchiveRDD private (val warcPath: String, parent: RDD[(String, ResolvedCdxRecord)]) extends ResolvedArchiveRDD[(String, ResolvedCdxRecord)](parent) {
  override protected def record(cdxWithPath: (String, ResolvedCdxRecord)) = {
    val (path, cdx) = cdxWithPath
    new ResolvedHdfsArchiveRecord(cdx, path)
  }
}