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

package de.l3s.archivespark.implicits.classes

import de.l3s.archivespark._
import de.l3s.archivespark.cdx.{CdxRecord, LocationInfo, ResolvedCdxRecord}
import org.apache.spark.rdd.RDD

import implicits._

import scala.reflect.ClassTag

class ResolvableRDD[Record <: ArchiveRecord : ClassTag](rdd: RDD[Record]) {
  def resolve(cdx: ResolvedCdxRecord) = rdd.map(r => r.resolve(cdx))

  def resolve(original: RDD[Record], fileMapping: RDD[String]): RDD[ResolvedArchiveRecord] = {
    val pairedFileMapping = fileMapping.map { r =>
      val split = r.split("\\s+")
      (split(0), split(1))
    }.reduceByKey((r1, r2) => r1)

    val revisitMime = "warc/revisit"
    val originalPaired = original.filter(r => r.mime != revisitMime).map(r => (r.digest, r)).reduceByKey { (r1, r2) =>
      if (r1.timestamp.compareTo(r2.timestamp) >= 0) r1 else r2
    }

    val (responses, revisits) = (rdd.filter(r => r.mime != revisitMime), rdd.filter(r => r.mime == revisitMime))
    val joined = revisits.map(r => (r.digest, r)).join(originalPaired).map(t => t._2)

    val joinedParentFilesWithKey = joined.map{ case (revisit, parent) => (parent.location.filename, (revisit, parent)) }.join(pairedFileMapping)
    val joinedParentFiles = joinedParentFilesWithKey.map{ case (_, t) => (t._1._1, (t._1._2, t._2))}

    val union = joinedParentFiles.union(responses.map(r => (r, (null, null)).asInstanceOf[Tuple2[Record, Tuple2[Record, String]]]))
    val joinedFilesWithKey = union.map { case (record, parent) => (record.location.filename, (record, parent)) }.join(pairedFileMapping)
    val joinedFiles = joinedFilesWithKey.map{ case (_, t) => ((t._1._1, t._2) , t._1._2) }

    joinedFiles.map { t =>
      val record = t._1._1
      val recordLocation = t._1._2
      val parent = t._2._1
      val parentLocation = t._2._2

      val parentCdx = new CdxRecord(
        parent.surtUrl,
        parent.timestamp,
        parent.originalUrl,
        parent.mime,
        parent.status,
        parent.digest,
        parent.redirectUrl,
        parent.meta,
        new LocationInfo(parent.location.compressedSize, parent.location.offset, parent.location.filename, parentLocation)
      )

      val resolvedCdx = new ResolvedCdxRecord(record, recordLocation, parentCdx)
      record.resolve(resolvedCdx)
    }
  }
}
