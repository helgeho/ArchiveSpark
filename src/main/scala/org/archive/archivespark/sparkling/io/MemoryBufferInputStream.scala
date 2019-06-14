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

import java.io.{ByteArrayOutputStream, InputStream, SequenceInputStream}
import java.util.Collections

import scala.collection.JavaConverters._

class MemoryBufferInputStream(stream: InputStream) extends InputStream {
  private val MaxSkipBufferSize = 2048 // InputStream.MAX_SKIP_BUFFER_SIZE

  private var inStream: InputStream = stream
  private var in: InputStream = inStream
  private var buffered = new ByteArray
  private val buffer = new ByteArrayOutputStream()

  private var marker: Option[MemoryBufferInputStream] = None
  private var markLimit: Int = -1
  private var markBuffered = new ByteArray

  private def writeBuffer(b: Int): Int = {
    if (b >= 0) {
      if (marker.isDefined) {
        markLimit -= 1
        if (markLimit < 0) applyMarker()
      }
      if (buffer.size.toLong + 1 > Int.MaxValue) {
        if (marker.isDefined) markBuffered.append(buffer.toByteArray)
        else buffered.append(buffer.toByteArray)
        buffer.reset()
      }
      buffer.write(b)
    }
    b
  }

  private def writeBuffer(array: Array[Byte], offset: Int, length: Int): Int = {
    if (length > 0) {
      if (marker.isDefined) {
        markLimit -= length
        if (markLimit < 0) applyMarker()
      }
      if (buffer.size.toLong + length > Int.MaxValue) {
        if (marker.isDefined) markBuffered.append(buffer.toByteArray)
        else buffered.append(buffer.toByteArray)
        buffer.reset()
      }
      buffer.write(array, offset, length)
    }
    length
  }

  override def read(): Int = writeBuffer(in.read())
  override def read(b: Array[Byte]): Int = writeBuffer(b, 0, in.read(b))
  override def read(b: Array[Byte], off: Int, len: Int): Int = writeBuffer(b, off, in.read(b, off, len))
  override def skip(n: Long): Long = read(new Array[Byte](n.min(MaxSkipBufferSize).toInt))
  def skip(n: Long, buffer: Boolean): Long = if (buffer) skip(n) else in.skip(n)
  override def available(): Int = in.available()
  def close(underlying: Boolean): Unit = {
    if (marker.isDefined) {
      marker.get.close(underlying = false)
      marker = None
    }
    inStream = null
    in = null
    buffered = null
    markBuffered = null
    buffer.close()
    if (underlying) stream.close()
  }
  override def close(): Unit = close(underlying = true)

  override def markSupported(): Boolean = true

  private def applyMarker(): Unit = if (marker.isDefined) {
    buffered.append(markBuffered)
    markBuffered = new ByteArray
    marker.get.close(underlying = false)
    marker = None
    markLimit = -1
    in = inStream
  }

  override def mark(readlimit: Int): Unit = {
    if (marker.isDefined) applyMarker()
    buffered.append(buffer.toByteArray)
    buffer.reset()
    markLimit = readlimit
    marker = Some(new MemoryBufferInputStream(inStream))
    in = marker.get
  }

  override def reset(): Unit = if (marker.isDefined) {
    markBuffered = new ByteArray
    buffer.reset()
    val markedBytes = marker.get.bytes
    if (markedBytes.nonEmpty) inStream = new SequenceInputStream(Collections.enumeration(Seq(markedBytes.toInputStream, inStream).asJava))
    in = inStream
    marker.get.close(underlying = false)
    marker = None
    markLimit = -1
  }

  def bytes: ByteArray = {
    val bytes = buffered.copy()
    if (marker.isDefined) bytes.append(markBuffered)
    bytes.append(buffer.toByteArray)
    bytes
  }

  def resetBuffer(): ByteArray = {
    val bytes = buffered
    buffered = new ByteArray
    if (marker.isDefined) {
      bytes.append(markBuffered)
      markBuffered = new ByteArray
    }
    bytes.append(buffer.toByteArray)
    buffer.reset()
    bytes
  }
}