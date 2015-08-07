package de.l3s.archivespark

/**
 * Created by holzmann on 04.08.2015.
 */
trait Enrichable[T, Field <: Enrichable[_, Field]] {
  abstract def value: T

  val enrichments = Map[String, Field]()

  def get[D](key: String): Enrichable[D, Field] = enrichments(key).asInstanceOf[Enrichable[D, Field]]

  //def enrich
}
