package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichFunc

/**
 * Created by holzmann on 04.08.2015.
 */
trait Enrichable[T, This <: Enrichable[T, This]] {
  def get: T

  private var _enrichments = Map[String, Enrichable[_, _]]()
  def enrichments = _enrichments

  def get[D](key: String): Enrichable[D, _] = enrichments(key).asInstanceOf[Enrichable[D, _]]

  def copy(): Any

  protected def enrich(f: EnrichFunc[_, _], path: Seq[String]): This = {
    if (path.isEmpty) return enrichThis(f.asInstanceOf[EnrichFunc[_, This]])
    val field = f.source.head
    val enriched = enrichments(field).enrich(f, f.source.tail).asInstanceOf[Enrichable[_, _]]
    val clone = copy().asInstanceOf[This]
    clone._enrichments = clone._enrichments.updated(field, enriched)
    clone
  }

  private def enrichThis(f: EnrichFunc[_, This]): This = {
    val clone = copy().asInstanceOf[Enrichable[_, _]]
    clone._enrichments ++= f.derive(clone.asInstanceOf[This])
    clone.asInstanceOf[This]
  }

  def toJson: Map[String, Any]
}
