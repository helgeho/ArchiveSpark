package de.l3s.archivespark.enrich

import de.l3s.archivespark.utils.IdentityMap
import de.l3s.archivespark.{Enrichable, EnrichRoot}

/**
 * Created by holzmann on 05.08.2015.
 */
trait EnrichFunc[Root <: EnrichRoot[_, Root], Source <: Enrichable[_, Source]] {
  def source: Seq[String]

  def derive(source: Source): Map[String, Enrichable[_, _]]

  def field: IdentityMap[String] = IdentityMap[String]()

  def checkExistence(root: Root): Boolean = {
    var field: Enrichable[_, _] = root
    for (key <- source) root.enrichments.get(key) match {
      case Some(nextField) => field = nextField
      case None => return false
    }
    true
  }
}
