package de.l3s.archivespark

import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.spark.SparkContext

/**
 * Created by holzmann on 04.08.2015.
 */
class HdfsArchiveRecord(rdd: HdfsArchiveRDD, cdx: CdxRecord)(implicit sc: SparkContext) extends ArchiveRecord(cdx) {
  protected lazy val bytes: Array[Byte] = {
    if (cdx.size < 0 || cdx.offset < 0) null
    else {
      val stream = sc.binaryFiles(s"hdfs://${rdd.warcPath}/${cdx.filename}}").first()._2
      try {
        val dataStream = stream.open()
        dataStream.skip(cdx.offset)
        val boundedStream = new BoundedInputStream(dataStream, cdx.size)
        IOUtils.toByteArray(boundedStream)
      } catch {
        case e: Exception => null // something went wrong, return null
      } finally {
        stream.close()
      }
    }
  }
}
