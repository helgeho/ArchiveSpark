package de.l3s.archivespark

import java.io.InputStream

import org.apache.commons.io.input.BoundedInputStream
import org.apache.spark.SparkContext

/**
 * Created by holzmann on 04.08.2015.
 */
class ResolvedHdfsArchiveRecord(rdd: HdfsArchiveRDD, cdx: ResolvedCdxRecord)(implicit sc: SparkContext) extends ResolvedArchiveRecord(cdx) {
  override def access[R >: Null](action: (String, InputStream) => R): R = {
    if (cdx.location.compressedSize < 0 || cdx.location.offset < 0) null
    else {
      val stream = sc.binaryFiles(s"${rdd.warcPath}/${cdx.location.filename}}").first()._2
      try {
        val dataStream = stream.open()
        dataStream.skip(cdx.location.offset)
        action(cdx.location.filename, new BoundedInputStream(dataStream, cdx.location.compressedSize))
      } catch {
        case e: Exception => null /* something went wrong, do nothing */
      } finally {
        stream.close()
      }
    }
  }
}
