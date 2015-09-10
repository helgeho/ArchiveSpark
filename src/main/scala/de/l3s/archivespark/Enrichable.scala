package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichFunc

/**
 * Created by holzmann on 04.08.2015.
 */
trait Enrichable[T, This <: Enrichable[_, _, Children], Children <: Enrichable[_, _, Children]] {
  def get: T

  private var _enrichments = Map[String, Children]()
  def enrichments = _enrichments

  def get[D](key: String): Enrichable[D, _, Children] = enrichments(key).asInstanceOf[Enrichable[D, Children]]

  protected def enrich(f: EnrichFunc[_, _, _], path: Seq[String]): This = {
    if (path.isEmpty) return enrichThis(f.asInstanceOf[EnrichFunc[_, This, Children]])
    val field = f.source.head
    val enriched = get(field).enrich(f, f.source.tail).asInstanceOf[Children]
    val clone = this.clone().asInstanceOf[This]
    clone._enrichments ++= Map(field -> enriched)
    clone
  }

  private def enrichThis(f: EnrichFunc[_, This, Children]): This = {
    val clone = this.clone().asInstanceOf[This]
    clone._enrichments ++= f.derive(this.asInstanceOf[This])
    clone
  }

  def toJson: Map[String, Any]
}
