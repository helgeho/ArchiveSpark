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

package de.l3s.archivespark.benchmarking

import java.io.{FileOutputStream, PrintStream}

class BenchmarkLogger(val filename: String) {
  private var lastName: String = null

  def log[T](result: BenchmarkResult[T], logValues: Boolean = false): T = {
    val stream = new FileOutputStream(filename, true)
    val out = new PrintStream(stream, true)
    if (result.name != lastName) out.println(result.name)
    val times = (if (logValues) result.measures.map(m => s"${m.value},${m.seconds}") else result.measures.map(m => m.seconds)).mkString("\t")
    out.println(s"\t${result.id}\t$times")
    out.close()
    stream.close()
    lastName = result.name
    result.measures.last.value
  }
}
