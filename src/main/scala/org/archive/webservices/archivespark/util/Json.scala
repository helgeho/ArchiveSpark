/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.util

import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Json => Circe}

import scala.collection.immutable.ListMap

object Json extends Serializable {
  val SingleValueKey = "_"

  def toJsonString(json: Circe, pretty: Boolean = true): String = if (pretty) json.spaces4 else json.noSpaces

  def mapToJson(map: Map[String, Circe]): Circe = {
    if (map == null || map.isEmpty) return null
    if (map.size == 1 && map.keys.head == null) map.values.head
    else ListMap(map.toSeq.filter{case (key, value) => value != null}.map{ case (key, value) => if (key == null) (SingleValueKey, value) else (key, value) }: _*).asJson
  }

  def json[A](obj: A): Circe = if (obj == null) Circe.Null else obj match {
    case json: Circe => json
    case json: JsonConvertible => json.toJson.asJson
    case map: Map[_, _] => map.map{case (k, v) => (k.toString, json(v))}.asJson
    case bytes: Array[Byte] => s"bytes(length: ${bytes.length})".asJson
    case iterable: TraversableOnce[_] => iterable.map(e => json(e)).toSeq.asJson
    case array: Array[_] => array.map(e => json(e)).asJson
    case str: String => str.asJson
    case n: Long => n.asJson
    case n: Int => n.asJson
    case n: Double => n.asJson
    case n: Float => n.asJson
    case n: Boolean => n.asJson
    case _ => s"obj:${obj.hashCode}".asJson
  }
}
