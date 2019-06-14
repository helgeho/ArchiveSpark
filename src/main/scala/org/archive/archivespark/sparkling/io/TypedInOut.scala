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

import java.io.{InputStream, OutputStream}

trait TypedInOut[A] extends Serializable {
  trait TypedInOutWriter {
    def stream: OutputStream
    def write(record: A)
    def flush(): Unit
    def close(): Unit
  }

  def out(stream: OutputStream): TypedInOutWriter
  def in(stream: InputStream): Iterator[A]
}

object TypedInOut {
  def apply[A, O](writer: OutputStream => O, reader: InputStream => Iterator[A])(writeRecord: (A, O) => Unit, flushOut: O => Unit, closeOut: O => Unit): TypedInOut[A] = new TypedInOut[A] {
    override def out(outStream: OutputStream): TypedInOutWriter = new TypedInOutWriter {
      override val stream: OutputStream = outStream
      private val out = writer(stream)
      override def write(record: A): Unit = writeRecord(record, out)
      override def flush(): Unit = flushOut(out)
      override def close(): Unit = closeOut(out)
    }

    override def in(inStream: InputStream): Iterator[A] = reader(inStream)
  }

  implicit val stringInOut: TypedInOut[String] = TypedInOut(IOUtil.print(_), IOUtil.lines(_))(
    (r, o) => o.println(r),
    _.flush(),
    _.close()
  )

  def toStringInOut[A](toString: A => String, fromString: String => A): TypedInOut[A] = TypedInOut(IOUtil.print(_), IOUtil.lines(_).map(fromString))(
    (r, o) => o.println(toString(r)),
    _.flush(),
    _.close()
  )
}