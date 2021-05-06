package org.archive.archivespark.sparkling.warc

import java.io._

import com.google.common.io.CountingInputStream
import org.apache.spark.rdd.RDD
import org.archive.archivespark.sparkling.io.{ByteArray, GzipUtil, IOUtil, MemoryBufferInputStream}
import org.archive.archivespark.sparkling.util.{IteratorUtil, RddUtil}

import scala.util.Try

object WarcLoader {
  def loadBytes(in: InputStream): Iterator[(Long, ByteArray)] = {
    var pos = 0L
    val buffered = new MemoryBufferInputStream(in)
    val counting = new CountingInputStream(buffered)
    if (GzipUtil.isCompressed(counting)) {
      GzipUtil.decompressConcatenated(counting).map { s =>
        IOUtil.readToEnd(s, close = true)
        val offset = pos
        pos = counting.getCount
        (offset, buffered.resetBuffer())
      }
    } else {
      IteratorUtil.whileDefined {
        WarcRecord.next(counting)
      }.map { warc =>
        warc.close()
        val offset = pos
        pos = counting.getCount
        (offset, buffered.resetBuffer())
      }
    }
  }

  def load(in: InputStream): Iterator[WarcRecord] = {
    var current: Option[WarcRecord] = None
    if (GzipUtil.isCompressed(in)) {
      GzipUtil.decompressConcatenated(in).flatMap { s =>
        IteratorUtil.whileDefined {
          if (current.isDefined) current.get.close()
          current = Try(WarcRecord.next(s)).getOrElse(None)
          current
        }
      }
    } else {
      IteratorUtil.whileDefined {
        if (current.isDefined) current.get.close()
        current = WarcRecord.next(in)
        current
      }
    }
  }

  def load(hdfsPath: String): RDD[WarcRecord] = RddUtil.loadBinary(hdfsPath, decompress = false, close = false) { (_, in) =>
    IteratorUtil.cleanup(load(in), in.close)
  }
}
