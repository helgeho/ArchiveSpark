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
