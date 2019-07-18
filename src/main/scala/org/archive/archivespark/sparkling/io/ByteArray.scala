/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
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

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream, SequenceInputStream}
import java.util.Collections

import scala.collection.JavaConverters._

class ByteArray {
  private val arrays = collection.mutable.Buffer.empty[Array[Byte]]

  def append(array: Array[Byte]): Unit = if (array.nonEmpty) arrays += array
  def append(array: ByteArray): Unit = if (array.nonEmpty) arrays ++= array.arrays

  def toInputStream: InputStream = new BufferedInputStream(new SequenceInputStream(Collections.enumeration(arrays.map(new ByteArrayInputStream(_)).asJava)))

  def length: Long = arrays.map(_.length.toLong).sum

  def isEmpty: Boolean = length == 0
  def nonEmpty: Boolean = length > 0

  def copy(): ByteArray = {
    val copy = new ByteArray()
    copy.append(this)
    copy
  }
}