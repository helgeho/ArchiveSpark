package de.l3s.archivespark.enrich

import de.l3s.archivespark.utils.JsonConvertible

import scala.reflect.ClassTag

/**
 * Created by holzmann on 04.08.2015.
 */
trait Enrichable[T, This <: Enrichable[T, This]] extends Cloneable with Serializable with JsonConvertible {
  private var excludeFromOutput: Option[Boolean] = None

  def get: T

  def isExcludedFromOutput: Boolean = excludeFromOutput match {
    case Some(value) => value
    case None => false
  }

  def excludeFromOutput(value: Boolean = true, overwrite: Boolean = true): Unit = excludeFromOutput match {
    case Some(value) => if (overwrite) excludeFromOutput = Some(value)
    case None => excludeFromOutput = Some(value)
  }

  private[enrich] var _enrichments = Map[String, Enrichable[_, _]]()
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
}
