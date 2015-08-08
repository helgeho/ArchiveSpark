package de.l3s.archivespark.enrich

import java.io.ByteArrayOutputStream

import de.l3s.archivespark.{ArchiveRecordField, ResolvedArchiveRecord}
import org.archive.io.ArchiveReaderFactory

/**
 * Created by holzmann on 05.08.2015.
 */
object Response extends EnrichFunc[ResolvedArchiveRecord, ResolvedArchiveRecord, ArchiveRecordField] {
  override def source: Seq[String] = Seq()

  override def derive(source: ResolvedArchiveRecord): Map[String, ArchiveRecordField[_]] = {
    source.access { case (fileName, stream) =>
      val reader = ArchiveReaderFactory.get(fileName, stream, false)
      val record = reader.get
      var header = record.getHeader
      val recordOutput: ByteArrayOutputStream = new ByteArrayOutputStream
      record.dump(recordOutput)
      recordOutput.close()
      // TODO: finish this
      Map(
        "warcHeader" -> new ArchiveRecordField(header.getHeaderFields),
        "httpHeader" -> new ArchiveRecordField(null),
        "payload" -> new ArchiveRecordField(null)
      )
    }
  }
}