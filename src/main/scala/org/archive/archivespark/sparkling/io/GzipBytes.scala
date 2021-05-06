package org.archive.archivespark.sparkling.io

import java.io._
import java.util.zip.GZIPOutputStream

class GzipBytes {
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
    print = IOUtil.print(open())
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

object GzipBytes {
  def open(action: OutputStream => Unit): Array[Byte] = {
    val bytes = new GzipBytes()
    action(bytes.open())
    bytes.close()
  }

  def openData(action: DataOutputStream => Unit): Array[Byte] = {
    val bytes = new GzipBytes()
    action(bytes.openData())
    bytes.close()
  }

  def openPrint(action: PrintStream => Unit): Array[Byte] = {
    val bytes = new GzipBytes()
    action(bytes.openPrint())
    bytes.close()
  }

  def apply(bytes: Array[Byte]): Array[Byte] = open(_.write(bytes))
}