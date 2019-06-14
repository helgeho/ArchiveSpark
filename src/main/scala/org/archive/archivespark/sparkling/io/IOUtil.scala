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

import java.io._

import com.google.common.io.FileBackedOutputStream
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader
import org.archive.archivespark.sparkling.util.{Common, IteratorUtil, ManagedVal, ValueSupplier}

import scala.util.Try

object IOUtil {
  import org.archive.archivespark.sparkling.Sparkling._

  var memoryBuffer: Int = prop(1024 * 1024 * 100)(memoryBuffer, memoryBuffer = _) // 100 MB

  def tmpFile: File = tmpFile()

  def tmpFile(prefix: String = tmpFilePrefix, ext: String = TmpExt, path: Option[String] = None, deleteOnExit: Boolean = true): File = {
    val file = path match {
      case Some(p) =>
        val dir = new File(p).getCanonicalFile
        dir.mkdirs()
        File.createTempFile(prefix, ext, dir)
      case None => File.createTempFile(prefix, ext)
    }
    if (deleteOnExit) file.deleteOnExit()
    file
  }

  def copy(in: InputStream, out: OutputStream, length: Long = -1): Unit = {
    if (length < 0) IOUtils.copy(in, out)
    else IOUtils.copy(new BoundedInputStream(in, length), out)
  }

  def copyToBuffer(in: InputStream, length: Long = -1, bufferSize: Int = memoryBuffer): ManagedVal[ValueSupplier[InputStream]] = buffer(bufferSize, lazyEval = false) { buffer =>
    copy(in, buffer, length)
  }

  def bytes(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    copy(in, out)
    out.close()
    out.toByteArray
  }

  def buffer(bufferSize: Int, lazyEval: Boolean)(write: OutputStream => Unit): ManagedVal[ValueSupplier[InputStream]] = {
    lazy val streams = collection.mutable.Buffer.empty[InputStream]
    lazy val buffer = new FileBackedOutputStream(bufferSize, true)
    ManagedVal({
      try {
        write(buffer)
      } finally {
        buffer.close()
      }
      ValueSupplier {
        Common.touch(buffer.getSupplier.getInput)(streams += _)
      }
    }, { _ =>
      streams.foreach(_.close())
      buffer.reset()
    }, lazyEval)
  }

  def buffer(write: OutputStream => Unit, file: Boolean = false, lazyEval: Boolean = false): ManagedVal[ValueSupplier[InputStream]] = {
    if (file) fileBuffer(write, lazyEval) else buffer(memoryBuffer, lazyEval)(write)
  }

  def fileBuffer(write: OutputStream => Unit, lazyEval: Boolean = false): ManagedVal[ValueSupplier[InputStream]] = {
    lazy val streams = collection.mutable.Buffer.empty[InputStream]
    lazy val file = IOUtil.tmpFile
    ManagedVal({
      val stream = new FileOutputStream(file)
      try {
        write(stream)
      } finally {
        stream.close()
      }
      ValueSupplier {
        Common.touch(new MemoryBufferInputStream(new FileInputStream(file)))(streams += _)
      }
    }, { _ =>
      streams.foreach(_.close())
      file.delete()
    }, lazyEval)
  }

  def decompress(in: InputStream, filename: Option[String] = None, checkFile: Boolean = false): InputStream = GzipUtil.decompress(in, filename, checkFile)

  def lines(file: String): Iterator[String] = {
    val in = new FileInputStream(file)
    IteratorUtil.cleanup(lines(in, Some(file)), in.close)
  }

  def lines(in: InputStream, filename: Option[String] = None): Iterator[String] = {
    val text = new Text()
    val reader = new LineReader(IOUtil.decompress(in, filename, checkFile = true))
    IteratorUtil.whileDefined {
      if (Try(reader.readLine(text)).getOrElse(0) == 0) None
      else Some(text.toString)
    }
  }

  def writeLines(file: String, lines: TraversableOnce[String]): Long = {
    val out = new FileOutputStream(file)
    val processed = writeLines(out, lines)
    out.close()
    processed
  }

  def writeLines(out: OutputStream, lines: TraversableOnce[String]): Long = {
    val stream = print(out)
    val processed = lines.map { line =>
      stream.println(line)
      1L
    }.sum
    stream.flush()
    processed
  }

  def eof(in: InputStream, markReset: Boolean = true): Boolean = {
    if (markReset) in.mark(1)
    val eof = try {
      in.read() == -1
    } catch {
      case e: EOFException => true
    }
    if (markReset && !eof) in.reset()
    eof
  }

  def emptyStream: InputStream = new ByteArrayInputStream(Array.empty)

  def read(in: InputStream, bytes: Array[Byte]): Boolean = {
    var read = 0
    val length = bytes.length
    while({
      val readNow = in.read(bytes, read, length - read)
      if (readNow == -1) return false
      read += readNow
      read < length
    }) { /* do nothing */ }
    true
  }

  def readToEnd(in: InputStream, close: Boolean = true): Unit = Try {
    while (in.read() != -1) in.skip(Long.MaxValue)
    if (close) in.close()
  }

  def skip(in: InputStream, bytes: Long): Long = {
    var skipped = 0L
    if (bytes > 0) {
      var prevSkip = 1L
      while (skipped < bytes && {
        val thisSkip = try {
          in.skip(bytes - skipped)
        } catch {
          case e: EOFException => -1
        }
        if (thisSkip == -1 || (prevSkip == 0 && thisSkip == 0)) false
        else {
          skipped += thisSkip
          prevSkip = thisSkip
          true
        }
      }) {}
    }
    skipped
  }

  def splitStream(in: InputStream, positions: TraversableOnce[(Long, Long)]): Iterator[InputStream] = {
    var currentStream: Option[BoundedInputStream] = None
    var position = 0L
    positions.map { case (offset, length) =>
      if (currentStream.isDefined) IOUtil.readToEnd(currentStream.get)

      IOUtil.skip(in, offset - position)
      position = offset + length

      currentStream = Some({
        val bounded = new BoundedInputStream(in, length)
        bounded.setPropagateClose(false)
        bounded
      })
      currentStream.get
    }.toIterator
  }

  def print(out: OutputStream, autoFlush: Boolean = false, closing: Boolean = true): PrintStream = new PrintStream(if (closing) out else new NonClosingOutputStream(out), autoFlush, DefaultCharset)
}
