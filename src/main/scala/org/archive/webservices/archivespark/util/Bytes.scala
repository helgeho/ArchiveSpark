/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2024 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.util

import org.apache.commons.io.input.BoundedInputStream
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.util.IteratorUtil

import java.io.{ByteArrayInputStream, InputStream}

trait Bytes {
  def array(length: Long, maxLength: Boolean = false): Array[Byte]
  def stream: InputStream
  override def toString: String = "[bytes]"
}

object Bytes {
  private class BytesArray (bytes: Array[Byte]) extends Bytes {
    def array(length: Long, maxLength: Boolean = false): Array[Byte] = if (maxLength && bytes.length > length) {
      IteratorUtil.take(bytes.toIterator, length).toArray
    } else bytes

    def stream = new ByteArrayInputStream(bytes)
  }

  private class BytesStream (in: => InputStream) extends Bytes {
    def array(length: Long, maxLength: Boolean = false): Array[Byte] = {
      val stream = in
      try {
        if (length < 0) IOUtil.bytes(stream)
        else IOUtil.bytes(new BoundedInputStream(stream, length))
      } finally {
        stream.close()
      }
    }

    def stream: InputStream = in
  }

  private class BytesEither(either: => Either[Array[Byte], InputStream]) extends Bytes {
    def array(length: Long, maxLength: Boolean = false): Array[Byte] = either match {
      case Left(bytes) => if (maxLength && bytes.length > length) {
        IteratorUtil.take(bytes.toIterator, length).toArray
      } else bytes
      case Right(in) =>
        try {
          if (length < 0) IOUtil.bytes(in)
          else IOUtil.bytes(new BoundedInputStream(in, length))
        } finally {
          in.close()
        }
    }

    def stream: InputStream = either match {
      case Left(bytes) => new ByteArrayInputStream(bytes)
      case Right(in) => in
    }
  }

  def apply(bytes: Array[Byte]): Bytes = new BytesArray(bytes)

  def apply(in: => InputStream): Bytes = new BytesStream(in)

  def either(either: => Either[Array[Byte], InputStream]): Bytes = new BytesEither(either)
}