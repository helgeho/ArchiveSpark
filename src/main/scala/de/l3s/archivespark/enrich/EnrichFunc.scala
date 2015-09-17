package de.l3s.archivespark.enrich

import de.l3s.archivespark.utils.IdentityMap

/**
 * Created by holzmann on 05.08.2015.
 */
trait EnrichFunc[Root <: EnrichRoot[_, Root], Source <: Enrichable[_, Source]] extends Serializable {
  def source: Seq[String]
  def fields: Seq[String]

  def enrich(root: Root): Root = {
    if (exists(root)) return root
    enrichPath(root, source).asInstanceOf[Root]
  }

  private def enrichPath(current: Enrichable[_, _], path: Seq[String]): Enrichable[_, _] = {
    if (path.isEmpty) enrichSource(current.asInstanceOf[Source])
    else {
      val field = path.head
      val enrichedField = enrichPath(current._enrichments(field), path.tail)
      val clone = current.copy().asInstanceOf[Enrichable[_, _]]
      clone._enrichments = clone._enrichments.updated(field, enrichedField)
      clone
    }
  }

  private def enrichSource(source: Source): Source = {
    val derivatives = new Derivatives[Enrichable[_, _]](fields)
    derive(source, derivatives)

    val clone = source.copy()
    clone._enrichments ++= derivatives.get
    clone
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
