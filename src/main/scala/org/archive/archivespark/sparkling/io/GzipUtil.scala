package org.archive.archivespark.sparkling.io

import java.io.{BufferedInputStream, InputStream}

import com.google.common.io.CountingInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.archive.archivespark.sparkling._
import org.archive.archivespark.sparkling.util.IteratorUtil

import scala.util.Try

object GzipUtil {
  import Sparkling._

  val Magic0 = 31
  val Magic1 = 139

  def isCompressed(in: InputStream): Boolean = {
    in.mark(2)
    val (b0, b1) = (in.read, in.read)
    in.reset()
    b0 == Magic0 && b1 == Magic1
  }

  def decompressConcatenated(in: InputStream): Iterator[InputStream] = decompressConcatenatedWithPosition(in).map{case (pos, s) => s}

  def decompressConcatenatedWithPosition(in: InputStream): Iterator[(Long, InputStream)] = {
    val stream = new CountingInputStream(IOUtil.supportMark(new NonClosingInputStream(in)))
    var prev: Option[InputStream] = None
    IteratorUtil.whileDefined {
      if (prev.isDefined) IOUtil.readToEnd(prev.get, close = true)
      if (IOUtil.eof(stream)) {
        stream.close()
        None
      } else Try {
        val pos = stream.getCount
        prev = Some(new GzipCompressorInputStream(new NonClosingInputStream(stream), false))
        prev.map((pos, _))
      }.getOrElse(None)
    }
  }

  def estimateCompressionFactor(in: InputStream, readUncompressedBytes: Long): Double = {
    val stream = new CountingInputStream(IOUtil.supportMark(new NonClosingInputStream(in)))
    val uncompressed = new GzipCompressorInputStream(stream, true)
    var read = IOUtil.skip(uncompressed, readUncompressedBytes)
    val decompressed = stream.getCount
    while (decompressed == stream.getCount && !IOUtil.eof(uncompressed, markReset = false)) read += 1
    val factor = read.toDouble / decompressed
    uncompressed.close()
    factor
  }

  def decompress(in: InputStream, filename: Option[String] = None, checkFile: Boolean = false): InputStream = {
    val buffered = IOUtil.supportMark(in)
    if (!IOUtil.eof(buffered) && ((filename.isEmpty && !checkFile) || (filename.isDefined && filename.get.toLowerCase.endsWith(GzipExt)))) {
      new GzipCompressorInputStream(buffered, true)
    } else buffered
  }
}
