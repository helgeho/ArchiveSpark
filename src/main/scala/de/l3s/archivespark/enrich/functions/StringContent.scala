package de.l3s.archivespark.enrich.functions

import de.l3s.archivespark.utils.IdentityMap
import de.l3s.archivespark.{ArchiveRecordField, ResolvedArchiveRecord}
import de.l3s.archivespark.enrich.{EnrichFunc, Enrichable, Derivatives, DependentEnrichFunc}
import org.apache.http.HttpEntity
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.util.EntityUtils

/**
 * Created by holzmann on 19.09.2015.
 */
object StringContent extends DependentEnrichFunc[ResolvedArchiveRecord, ArchiveRecordField[Array[Byte]]] {
  override def dependency: EnrichFunc[ResolvedArchiveRecord, _] = Response
  override def dependencyField: String = "content"

  override def fields: Seq[String] = Seq("string")
  override def field: IdentityMap[String] = IdentityMap(
    "text" -> "string"
  )

  override def derive(source: ArchiveRecordField[Array[Byte]], derivatives: Derivatives[Enrichable[_, _]]): Unit = {
    val entity: HttpEntity = new ByteArrayEntity(source.get)
    derivatives << ArchiveRecordField(EntityUtils.toString(entity).trim)
  }
}
