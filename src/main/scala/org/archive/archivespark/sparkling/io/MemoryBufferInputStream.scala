package org.archive.archivespark.sparkling.io

import java.io.{ByteArrayOutputStream, InputStream}

class MemoryBufferInputStream(in: InputStream) extends InputStream {
  private val MaxSkipBufferSize = 2048 // InputStream.MAX_SKIP_BUFFER_SIZE
  private val MaxArraySize = Int.MaxValue - 8 // ByteArrayOutputStream.MAX_ARRAY_SIZE

  private var stream: InputStream = IOUtil.supportMark(in)
  private var buffered = new ByteArray
  private val buffer = new ByteArrayOutputStream()

  private var markLimit: Int = -1
  private var markBuffered = new ByteArray

  private def writeBuffer(b: Int): Int = {
    if (b >= 0) {
      if (markLimit >= 0) {
        markLimit -= 1
        if (markLimit < 0) applyMarker()
      }
      if (buffer.size.toLong + 1 > MaxArraySize) {
        if (markLimit >= 0) markBuffered.append(buffer.toByteArray)
        else buffered.append(buffer.toByteArray)
        buffer.reset()
      }
      buffer.write(b)
    }
    b
  }

  private def writeBuffer(array: Array[Byte], offset: Int, length: Int): Int = {
    if (length > 0) {
      if (markLimit >= 0) {
        markLimit -= length
        if (markLimit < 0) applyMarker()
      }
      if (buffer.size.toLong + length > MaxArraySize) {
        if (markLimit >= 0) markBuffered.append(buffer.toByteArray)
        else buffered.append(buffer.toByteArray)
        buffer.reset()
      }
      buffer.write(array, offset, length)
    }
    length
  }

  override def read(): Int = writeBuffer(stream.read())
  override def read(b: Array[Byte]): Int = writeBuffer(b, 0, stream.read(b))
  override def read(b: Array[Byte], off: Int, len: Int): Int = writeBuffer(b, off, stream.read(b, off, len))

  override def skip(n: Long): Long = {
    if (n < 0) 0
    else {
      var skipped = 0L
      val skipBuffer = new Array[Byte](n.min(MaxSkipBufferSize).toInt)
      while (skipped < n) {
        val s = read(skipBuffer, 0, (n - skipped).min(MaxSkipBufferSize).toInt)
        if (s < 0) return skipped
        skipped += s
      }
      skipped
    }
  }
  def skip(n: Long, buffer: Boolean): Long = if (buffer) skip(n) else stream.skip(n)

  override def available(): Int = stream.available()
  override def close(): Unit = {
    buffered = null
    markBuffered = null
    buffer.close()
    stream.close()
  }

  override def markSupported(): Boolean = true

  private def applyMarker(): Unit = {
    buffered.append(markBuffered)
    markBuffered = new ByteArray
    markLimit = -1
  }

  override def mark(readlimit: Int): Unit = {
    if (markLimit >= 0) applyMarker()
    buffered.append(buffer.toByteArray)
    buffer.reset()
    stream.mark(readlimit)
    markLimit = readlimit
  }

  override def reset(): Unit = if (markLimit >= 0) {
    markBuffered = new ByteArray
    buffer.reset()
    stream.reset()
    markLimit = -1
  }

  def bytes: ByteArray = {
    val bytes = buffered.copy()
    if (markLimit >= 0) bytes.append(markBuffered)
    bytes.append(buffer.toByteArray)
    bytes
  }

  def resetBuffer(): ByteArray = {
    val bytes = buffered
    buffered = new ByteArray
    if (markLimit >= 0) {
      bytes.append(markBuffered)
      markBuffered = new ByteArray
    }
    bytes.append(buffer.toByteArray)
    buffer.reset()
    bytes
  }
}