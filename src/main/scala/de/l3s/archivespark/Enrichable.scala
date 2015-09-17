package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichFunc

import scala.reflect.ClassTag

/**
 * Created by holzmann on 04.08.2015.
 */
trait Enrichable[T, This <: Enrichable[T, This]] extends Cloneable with Serializable with JsonConvertible {
  def get: T

  private var _enrichments = Map[String, Enrichable[_, _]]()
  def enrichments = _enrichments

  def apply[D : ClassTag](key: String): Option[Enrichable[D, _]] = {
    def find(current: Enrichable[_, _], path: Seq[String]): Enrichable[_, _] = {
      if (path.isEmpty || (path.length == 1 && path.head == "")) return current
      if (path.head == "") {
        var target = find(this, path.drop(1))
        if (target != null) return target
        for (e <- _enrichments.values) {
          target = find(e, path)
          if (target != null) return target
        }
        null
      } else {
        current.enrichments.get(path.head) match {
          case Some(enrichable) => find(enrichable, path.drop(1))
          case None => null
        }
      }
    }
    val target = find(this, key.trim.split("\\."))
    if (target == null) return None
    target.get match {
      case _: D => Some(target.asInstanceOf[Enrichable[D, _]])
      case _ => None
    }
  }

  def get[D : ClassTag](key: String): Option[D] = apply[D](key) match {
    case Some(enrichable) => Some(enrichable.get)
    case None => None
  }

  def copy(): This

  protected def enrich(f: EnrichFunc[_, _], path: Seq[String]): This = {
    if (path.isEmpty) return enrichThis(f.asInstanceOf[EnrichFunc[_, This]])
    val field = path.head
    val enriched = enrichments(field).enrich(f, f.source.tail).asInstanceOf[Enrichable[_, _]]
    val clone = copy()
    clone._enrichments = clone._enrichments.updated(field, enriched)
    clone
  }

  private def enrichThis(f: EnrichFunc[_, This]): This = {
    val clone = copy()
    clone._enrichments ++= f.enrich(clone)
    clone
  }
}
