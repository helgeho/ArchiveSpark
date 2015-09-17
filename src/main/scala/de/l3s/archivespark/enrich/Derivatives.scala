package de.l3s.archivespark.enrich

/**
 * Created by holzmann on 16.09.2015.
 */
class Derivatives[T](val fields: Seq[String]) {
  private var nextField = 0
  private var map = Map[String, T]()

  def get: Map[String, T] = map

  def <<(value: T) = {
    map += fields(nextField) -> value
    nextField += 1
  }
}
