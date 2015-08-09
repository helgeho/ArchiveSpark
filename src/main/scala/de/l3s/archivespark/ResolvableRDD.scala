package de.l3s.archivespark

import org.apache.spark.rdd.RDD

/**
 * Created by holzmann on 05.08.2015.
 */
object ResolvableRDD {
  implicit class ResolvableRDD[Record <: ArchiveRecord](rdd: RDD[Record]) {
    def resolve(original: RDD[Record], fileMapping: RDD[String]): RDD[ResolvedArchiveRecord] = {
      val pairedFileMapping = fileMapping.map { r =>
        val split = r.split("\\s+")
        (split(0), split(1))
      }.reduceByKey { (r1, r2) => r1 }

      val revisitMime = "warc/revisit"
      val originalPaired = original.filter(r => r.cdx.mime != revisitMime).map(r => (r.cdx.digest, r)).reduceByKey { (r1, r2) =>
        if (r1.cdx.timestamp.compareTo(r2.cdx.timestamp) >= 0) r1 else r2
      }

      val (responses, revisits) = (rdd.filter(r => r.cdx.mime != revisitMime), rdd.filter(r => r.cdx.mime == revisitMime))
      val joined = revisits.map(r => (r.cdx.digest, r)).join(originalPaired).map(t => t._2)

      val joinedParentFilesWithKey = joined.map{ case (revisit, parent) => (parent.cdx.location.filename, (revisit, parent)) }.join(pairedFileMapping)
      val joinedParentFiles = joinedParentFilesWithKey.map{ case (_, t) => (t._1._1, (t._1._2, t._2))}

      val union = joinedParentFiles.union(responses.map(r => (r, (null, null)).asInstanceOf[Tuple2[Record, Tuple2[Record, String]]]))
      val joinedFilesWithKey = union.map { case (record, parent) => (record.cdx.location.filename, (record, parent)) }.join(pairedFileMapping)
      val joinedFiles = joinedFilesWithKey.map{ case (_, t) => ((t._1._1, t._2) , t._1._2) }

      joinedFiles.map { t =>
        val record = t._1._1
        val recordLocation = t._1._2
        val parent = t._2._1
        val parentLocation = t._2._2

        val parentCdx = CdxRecord(
          parent.cdx.surtUrl,
          parent.cdx.timestamp,
          parent.cdx.originalUrl,
          parent.cdx.mime,
          parent.cdx.status,
          parent.cdx.digest,
          parent.cdx.redirectUrl,
          parent.cdx.meta,
          new LocationInfo(parent.cdx.location.compressedSize, parent.cdx.location.offset, parent.cdx.location.filename, parentLocation)
        )

        val resolvedCdx = new ResolvedCdxRecord(record.cdx, recordLocation, parentCdx)
        record.resolve(resolvedCdx)
      }
    }
  }
}
