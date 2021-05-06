package org.archive.archivespark.sparkling.io

import java.io.{InputStream, OutputStream}

import org.archive.archivespark.sparkling.cdx.CdxRecord

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

  implicit val cdxInOut: TypedInOut[CdxRecord] = toStringInOut(_.toCdxString, CdxRecord.fromString(_).get)

  def toStringInOut[A](toString: A => String, fromString: String => A): TypedInOut[A] = TypedInOut(IOUtil.print(_), IOUtil.lines(_).map(fromString))(
    (r, o) => o.println(toString(r)),
    _.flush(),
    _.close()
  )
}