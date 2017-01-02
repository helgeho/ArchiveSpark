/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2017 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

import de.l3s.archivespark.enrich._

import scala.util.Try

private object JsonNamespace extends IdentityEnrichFunction(Root[String], "json")

class Json private (path: Seq[String], fieldName: String) extends BoundEnrichFunc[TypedEnrichRoot[String], String](JsonNamespace) with SingleField[Any] {
  override def fields = Seq(fieldName)

  override def derive(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    var jsonSource = Try{de.l3s.archivespark.utils.Json.jsonToMap(source.get)}
    for (key <- path if jsonSource.isSuccess) {
      jsonSource = jsonSource.map(_(key).asInstanceOf[Map[String, Any]])
    }
    if (jsonSource.isSuccess) derivatives.setNext(de.l3s.archivespark.utils.Json.mapToEnrichable(jsonSource.get, source))
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