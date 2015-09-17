package de.l3s.archivespark.enrich

import de.l3s.archivespark.utils.IdentityMap
import de.l3s.archivespark.{EnrichRoot, Enrichable}

/**
 * Created by holzmann on 05.08.2015.
 */
trait EnrichFunc[Root <: EnrichRoot[_, Root], Source <: Enrichable[_, Source]] extends Serializable {
  def source: Seq[String]
  def fields: Seq[String]

  def enrich(source: Source): Map[String, Enrichable[_, _]] = {
    val derivatives = new Derivatives[Enrichable[_, _]](fields)
    derive(source, derivatives)
    derivatives.get
  }

  def derive(source: Source, derivatives: Derivatives[Enrichable[_, _]]): Unit

  def field: IdentityMap[String] = IdentityMap[String]()

  def exists(root: Root): Boolean = {
    var field: Enrichable[_, _] = root
    for (key <- source) field.enrichments.get(key) match {
      case Some(nextField) => field = nextField
      case None => return false
    }
    if (!fields.forall(f => field.enrichments.contains(f))) return false
    true
  }
}
