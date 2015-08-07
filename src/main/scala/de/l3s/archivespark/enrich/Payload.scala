package de.l3s.archivespark.enrich

import de.l3s.archivespark.{ArchiveRecord, EnrichRootField}

/**
 * Created by holzmann on 05.08.2015.
 */
object Payload extends EnrichFunc[ArchiveRecord] with DependencyFunc {
  override def entry(record: ArchiveRecord): EnrichRootField[_, ArchiveRecord] = record.response


}