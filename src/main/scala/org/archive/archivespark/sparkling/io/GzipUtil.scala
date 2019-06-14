/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2018 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.archive.archivespark.sparkling.io

import java.io.{BufferedInputStream, InputStream}

import com.google.common.io.CountingInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.archive.archivespark.sparkling.Sparkling
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
    val stream = new CountingInputStream(new BufferedInputStream(new NonClosingInputStream(in)))
    var uncompressed: Option[InputStream] = None
    IteratorUtil.whileDefined {
      if (uncompressed.isDefined) IOUtil.readToEnd(uncompressed.get, close = true)
      if (IOUtil.eof(stream)) {
        stream.close()
        None
      } else Try {
        val pos = stream.getCount
        uncompressed = Some(new GzipCompressorInputStream(new NonClosingInputStream(stream), false))
        uncompressed.map((pos, _))
      }.getOrElse(None)
    }
  }

  def estimateCompressionFactor(in: InputStream, readUncompressedBytes: Long): Double = {
    val stream = new CountingInputStream(new BufferedInputStream(new NonClosingInputStream(in)))
    val uncompressed = new GzipCompressorInputStream(stream, true)
    var read = IOUtil.skip(uncompressed, readUncompressedBytes)
    val decompressed = stream.getCount
    while (decompressed == stream.getCount && !IOUtil.eof(uncompressed, markReset = false)) read += 1
    val factor = read.toDouble / decompressed
    uncompressed.close()
    factor
  }

  def decompress(in: InputStream, filename: Option[String] = None, checkFile: Boolean = false): InputStream = {
    val buffered = if (in.markSupported()) in else new BufferedInputStream(in)
    if (!IOUtil.eof(buffered) && ((filename.isEmpty && !checkFile) || (filename.isDefined && filename.get.toLowerCase.endsWith(GzipExt)))) {
      new GzipCompressorInputStream(buffered, true)
    } else buffered
  }
}
