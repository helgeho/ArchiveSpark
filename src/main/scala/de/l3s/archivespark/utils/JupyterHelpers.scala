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

import org.apache.spark.rdd.RDD

import scala.util.Random

object JupyterHelpers {
  private var renderJsonInitialized = false

  def printHtml(html: String) = Left(Map("text/html" -> html))
  def printText(text: String) = Left(Map("text/plain" -> text))

  def initRenderJson() = {
    renderJsonInitialized = true
    printHtml("""
      <script type="text/javascript" src="https://rawgit.com/caldwell/renderjson/master/renderjson.js"></script>
      <style type="text/css">
        .renderjson .disclosure    { color: black; font-weight: bold; text-decoration: none; }
        .renderjson .syntax        { color: black; }
        .renderjson .string        { color: darkgreen; }
        .renderjson .number        { color: purple; }
        .renderjson .boolean       { color: red; }
        .renderjson .key           { color: darkblue; }
        .renderjson .keyword       { color: orange; }
      </style>
    """)
  }

  def renderJson(json: String, showToLevel: Int = 1) = {
    val id = Random.nextInt(Int.MaxValue)
    printHtml(s"""
      <div id="$id"></div>
      <script>
        (function () {
          var render = renderjson.set_show_to_level($showToLevel).set_max_string_length(200);
          document.getElementById("$id").appendChild(
            render(${json.replace("<", "\\u003C").replace(">", "\\u003E")})
          );
        })();
      </script>
    """)
  }

  def peek[T <: JsonConvertible](rdd: RDD[T], showToLevel: Int = 1) = {
    if (renderJsonInitialized) {
      val json = rdd.take(1).headOption.map(_.toJsonString(false)).getOrElse("")
      renderJson(json, showToLevel)
    } else {
      printText(rdd.take(1).headOption.map(_.toJsonString(true)).getOrElse(""))
    }
  }
}
