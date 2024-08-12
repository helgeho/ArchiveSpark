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

package org.archive.webservices.archivespark.model

import io.circe.Json
import org.archive.webservices.archivespark.util.Json.{json, mapToJson}

import scala.collection.immutable.ListMap

trait TypedEnrichRoot[+Meta] extends EnrichRoot with TypedEnrichable[Meta] {
  override def root[A]: TypedEnrichRoot[A] = this.asInstanceOf[TypedEnrichRoot[A]]

  def companion: EnrichRootCompanion[_]
}

trait EnrichRoot extends Enrichable { this: TypedEnrichRoot[_] =>
  def metaKey: String = "record"

  def companion: EnrichRootCompanion[_]

  def metaToJson: Json = json(get)

  def toJson: Map[String, Json] = ListMap(
    metaKey -> metaToJson
  ) ++ enrichments.flatMap { key =>
    val e = enrichment(key).get
    if (e.isTransparent) {
      e.enrichments.map { key =>
        (key, mapToJson(e.enrichment(key).get.toJson))
      }
    } else {
      Iterator((key, mapToJson(e.toJson)))
    }
  }.filter{ case (_, field) => field != null }
}

object EnrichRoot {
  implicit def companion[A <: EnrichRoot](root: A): EnrichRootCompanion[A] = root.companion.asInstanceOf[EnrichRootCompanion[A]]
}