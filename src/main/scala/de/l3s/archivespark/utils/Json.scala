/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 */

package de.l3s.archivespark.utils

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._

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
