package de.l3s.archivespark.enrich.functions

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import de.l3s.archivespark.enrich.{Enrichable, Derivatives, EnrichFunc}
import de.l3s.archivespark.utils.IdentityMap
import de.l3s.archivespark.{ArchiveRecordField, ResolvedArchiveRecord}
import org.apache.commons.io.IOUtils
import org.archive.format.http.{HttpHeader, HttpResponseParser}
import org.archive.io.ArchiveReaderFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Created by holzmann on 05.08.2015.
 */
object Response extends EnrichFunc[ResolvedArchiveRecord, ResolvedArchiveRecord] {
  override def source: Seq[String] = Seq()
  override def fields = Seq("recordHeader", "httpHeader", "payload")

  override def field: IdentityMap[String] = IdentityMap(
    "content" -> "payload"
  )

  override def derive(source: ResolvedArchiveRecord, derivatives: Derivatives[Enrichable[_, _]]): Unit = {
    source.access { case (fileName, stream) =>
      val reader = ArchiveReaderFactory.get(fileName, stream, false)
      val record = reader.get
      val header = record.getHeader

      derivatives << ArchiveRecordField(header.getHeaderFields.asScala.toMap)

      var recordOutput: ByteArrayOutputStream = null
      try {
        recordOutput = new ByteArrayOutputStream
        record.dump(recordOutput)
      } finally {
        if (recordOutput != null) recordOutput.close()
      }

      var httpResponse: InputStream = null
      try {
        httpResponse = new ByteArrayInputStream(recordOutput.toByteArray)

        val parser = new HttpResponseParser
        val response = parser.parse(httpResponse)
        val httpHeaders = response.getHeaders

        val httpHeadersMap = mutable.Map[String, String]()
        for (httpHeader: HttpHeader <- httpHeaders.iterator().asScala) {
          httpHeadersMap.put(httpHeader.getName, httpHeader.getValue)
        }

        derivatives << ArchiveRecordField(httpHeadersMap.toMap)

        val payload = IOUtils.toByteArray(httpResponse)

        derivatives << ArchiveRecordField(payload)
      } finally {
        if (httpResponse != null) httpResponse.close()
      }
    }
  }
}