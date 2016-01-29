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

package de.l3s.archivespark

import de.l3s.archivespark.utils.Json
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class JsonSpec extends FlatSpec {
  "JSON of a Map" should "look like a JSON object with every key/value pair being a field with value" in {
    val map = Map("a" -> 1, "b" -> 2, "c" -> 3)
    val json = """{"a":1,"b":2,"c":3}"""

    val field = ArchiveRecordField(map)
  }

  "JSON of a byte array" should "be a string representation with length" in {
    val bytes = Array[Byte](1,2,3,4,5)
    val json = "\"bytes(length: 5)\""

    val field = ArchiveRecordField(bytes)
    field.toJsonString shouldEqual json
  }
}
