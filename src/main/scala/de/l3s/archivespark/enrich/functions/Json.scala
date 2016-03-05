/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.enrich.functions

import de.l3s.archivespark.ResolvedArchiveRecord
import de.l3s.archivespark.enrich._

private object JsonNamespace extends IdentityEnrichFunction(StringContent, "json")

class Json private (path: Seq[String], fieldName: String) extends BoundEnrichFunc[ResolvedArchiveRecord, Enrichable[String, _]](JsonNamespace)
  with SingleFieldEnrichFunc {
  override def fields = Seq(fieldName)

  private def mapToEnrichable(jsonMap: Map[String, Any], parent: Enrichable[_, _]): Enrichable[_, Enrichable[_, _]] = {
    val json = de.l3s.archivespark.utils.Json.mapToJson(jsonMap)
    var enrichable = new EnrichableImpl[String, Enrichable[_, _]](json, parent, parent.root).asInstanceOf[Enrichable[_, Enrichable[_, _]]]
    enrichable.excludeFromOutput(value = true)
    for ((key, value) <- jsonMap) {
      enrichable = value match {
        case valueMap: Map[String, Any] => enrichable.enrich(key, mapToEnrichable(valueMap, enrichable)).asInstanceOf[Enrichable[_, Enrichable[_, _]]]
        case _ => enrichable.enrichValue(key, value).asInstanceOf[Enrichable[_, Enrichable[_, _]]]
      }
    }
    enrichable
  }

  override def derive(source: Enrichable[String, _], derivatives: Derivatives): Unit = {
    var jsonSource = de.l3s.archivespark.utils.Json.jsonToMap(source.get)
    for (key <- path) {
      val next = jsonSource.getOrElse(key, null)
      jsonSource = next match {
        case nextSource: Map[String, Any] => nextSource
        case _ => null
      }
    }
    if (jsonSource != null) derivatives << mapToEnrichable(jsonSource, source)
  }
}

object Json extends Json(Seq(), "root") {
  def apply(path: String = "", fieldName: String = null) = {
    val pathSplit = if (path.isEmpty) Seq() else path.split("\\.").toSeq
    new Json(pathSplit, if (fieldName != null) fieldName else {
      if (pathSplit.isEmpty) "root"
      else pathSplit.reverse.head
    })
  }
}