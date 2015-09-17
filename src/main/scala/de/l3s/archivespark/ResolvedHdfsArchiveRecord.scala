package de.l3s.archivespark

import java.io.InputStream

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Created by holzmann on 04.08.2015.
 */
class ResolvedHdfsArchiveRecord(cdx: ResolvedCdxRecord, cdxPath: String = null) extends ResolvedArchiveRecord(cdx) {
  override def access[R >: Null](action: (String, InputStream) => R): R = {
    if (cdx.location.compressedSize < 0 || cdx.location.offset < 0) null
    else {
      val fs = FileSystem.get(SparkHadoopUtil.get.conf)
      findArchivePath(fs, cdx.location.fileLocation, cdx.location.filename) match {
        case None => null
        case Some(path) =>
          val stream = fs.open(path)
          try {
            stream.seek(cdx.location.offset)
            action(cdx.location.filename, new BoundedInputStream(stream, cdx.location.compressedSize))
          } catch {
            case e: Exception => null /* something went wrong, do nothing */
          } finally {
            stream.close()
          }
      }
    }
  }

  private def findArchivePath(fs: FileSystem, base: String, filename: String): Option[Path] = {
    var cdxBase = if (cdxPath == null) null else new Path(cdxPath).getParent
    var dir = new Path(base)
    var path = new Path(dir, filename)
    while (!fs.exists(path)) {
      if (cdxBase == null || cdxBase.depth() == 0) return None
      dir = new Path(dir, cdxBase.getName)
      cdxBase = cdxBase.getParent
      path = new Path(dir, filename)
    }
    Some(path)
  }
}
