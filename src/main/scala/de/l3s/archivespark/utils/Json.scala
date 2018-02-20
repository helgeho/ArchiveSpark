/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2018 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.l3s.archivespark.utils

import de.l3s.archivespark.enrich.{Enrichable, SingleValueEnrichable}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._

import scala.collection.immutable.ListMap
import scala.util.{Success, Try}

object Json extends Serializable {
  implicit val formats = DefaultFormats

  val SingleValueKey = "_"

  def mapToJson(map: Map[String, Any], pretty: Boolean = true): String = if (pretty) writePretty(mapToJsonValue(map)) else write(mapToJsonValue(map))
  def jsonToMap(json: String): Map[String, Any] = parse(json).extract[Map[String, Any]]

  def mapToJsonValue(map: Map[String, Any]): AnyRef = {
    if (map == null || map.isEmpty) return null
    if (map.size == 1 && map.keys.head == null) map.values.head.asInstanceOf[AnyRef]
    else ListMap(map.toSeq.map{ case (key, value) => if (key == null) (SingleValueKey, value) else (key, value) }: _*)
  }

  def json[A](obj: A): Any = obj match {
    case json: JsonConvertible => json.toJson
    case map: Map[_, _] => map.map{case (k, v) => (json(k), json(v))}
    case bytes: Array[Byte] => s"bytes(length: ${bytes.length})"
    case iterable: Iterable[_] => iterable.map(e => json(e))
    case _ => obj
  }

  def mapToEnrichable(jsonMap: Map[String, Any], parent: Enrichable, field: String): Enrichable = {
    val json = de.l3s.archivespark.utils.Json.mapToJson(jsonMap)
    var enrichable: Enrichable = SingleValueEnrichable[String](json)
    enrichable.excludeFromOutput()
    for ((key, value) <- jsonMap) {
      enrichable = Try{value.asInstanceOf[Map[String, Any]]} match {
        case Success(valueMap) => enrichable.enrich(key, mapToEnrichable(valueMap, enrichable, key))
        case _ => enrichable.enrichValue(key, value)
      }
    }
    enrichable
  }
}
