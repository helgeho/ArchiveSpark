package de.l3s.archivespark.enrich

import de.l3s.archivespark.{EnrichRoot, EnrichRootField}

/**
 * Created by holzmann on 05.08.2015.
 */
trait EnrichFunc[Root <: EnrichRoot[Root]] {
  def entry(record: Root): EnrichRootField[_, Root]
}
