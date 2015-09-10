package de.l3s.archivespark.utils

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._

/**
 * Created by holzmann on 10.09.2015.
 */
object Json {
  implicit val formats = DefaultFormats

  val SingleValueKey = "_"

  def mapToJson(map: Map[String, Any]): String = write(map)
  def jsonToMap(json: String): Map[String, Any] = parse(json).extract[Map[String, Any]]

  def mapToAny(map: Map[String, Any]): Any = {
    if (map.size == 1 && map.keys.head == null) map.values.head
    else map.map{ case (key, value) => if (key == null) (SingleValueKey, value) else (key, value) }
  }
}
