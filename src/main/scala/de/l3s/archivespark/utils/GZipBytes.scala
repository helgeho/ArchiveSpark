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

package de.l3s.archivespark.utils

import java.io._
import java.util.zip.GZIPOutputStream

class GZipBytes {
  private val bytes: ByteArrayOutputStream = new ByteArrayOutputStream()

  private var print: PrintStream = _
  private var compressed: GZIPOutputStream = _
  private var data: DataOutputStream = _

  def open(): OutputStream = {
    compressed = new GZIPOutputStream(bytes)
    compressed
  }

  def openData(): DataOutputStream = {
    data = new DataOutputStream(open())
    data
  }

  def openPrint(): PrintStream = {
    print = new PrintStream(open())
    print
  }

  def close(): Array[Byte] = {
    if (data != null) data.flush()
    if (print != null) print.flush()

    compressed.flush()
    compressed.finish()
    bytes.flush()

    val array = bytes.toByteArray

    if (data != null) data.close()
    if (print != null) print.close()

    data = null
    print = null

    compressed.close()
    bytes.reset()

    array
  }
}

object GZipBytes {
  def open(action: OutputStream => Unit): Array[Byte] = {
    val bytes = new GZipBytes()
    action(bytes.open())
    bytes.close()
  }

  def openData(action: DataOutputStream => Unit): Array[Byte] = {
    val bytes = new GZipBytes()
    action(bytes.openData())
    bytes.close()
  }

  def openPrint(action: PrintStream => Unit): Array[Byte] = {
    val bytes = new GZipBytes()
    action(bytes.openPrint())
    bytes.close()
  }

  def apply(bytes: Array[Byte]): Array[Byte] = open(_.write(bytes))
}
