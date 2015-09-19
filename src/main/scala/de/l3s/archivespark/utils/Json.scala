package de.l3s.archivespark.utils

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._

/**
 * Created by holzmann on 10.09.2015.
 */
object Json extends Serializable {
  implicit val formats = DefaultFormats

  val SingleValueKey = "_"

  def mapToJson(map: Map[String, Any], pretty: Boolean = true): String = if (pretty) writePretty(map) else write(map)
  def jsonToMap(json: String): Map[String, Any] = parse(json).extract[Map[String, Any]]

  def mapToAny(map: Map[String, Any]): Any = {
    if (map.isEmpty) return null
    if (map.size == 1 && map.keys.head == null) map.values.head
    else map.map{ case (key, value) => if (key == null) (SingleValueKey, value) else (key, value) }
  }

  def json[A](obj: A): Any = obj match {
    case json: JsonConvertible => json.toJson
    case map: Map[_, _] => map.map{case (k, v) => (json(k), json(v))}
    case bytes: Array[Byte] => s"bytes(length: ${bytes.length})"
    case iterable: Iterable[_] => iterable.map(e => json(e))
    case _ => obj
  }
}
