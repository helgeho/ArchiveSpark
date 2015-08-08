package de.l3s.archivespark.utils

/**
 * Created by holzmann on 07.08.2015.
 */
class IdentityMap[T](elems: (T, T)*) {
  val map = Map[T, T](elems: _*)

  def apply(key: T): T = {
    map.get(key) match {
      case Some(value) => value
      case None => key
    }
  }
}

object IdentityMap {
  def apply[T](elems: (T, T)*) = new IdentityMap[T](elems: _*)
}
