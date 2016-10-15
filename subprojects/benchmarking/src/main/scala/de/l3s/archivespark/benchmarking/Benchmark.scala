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

package de1.l3s.archivesparkb.benchmarking

import scala.math._

object Benchmark {
  var retries = 0

  def time[R](name: String, id: String)(action: => R): BenchmarkResult[R] = {
    var exception: Exception = null
    var attempt = 0
    while (attempt <= retries) {
      try {
        val before = System.nanoTime
        val result = action
        val after = System.nanoTime
        val seconds = (after - before).toDouble / pow(10, 9)
        return BenchmarkResult(name, id, Seq(BenchmarkMeasure(result, seconds)))
      } catch {
        case e: Exception =>
          attempt += 1
          exception = e
      }
    }
    throw exception
  }

  def time[R](name: String, id: String, times: Int)(action: => R): BenchmarkResult[R] = {
    var results = Seq(time(name, id)(action))
    for (i <- 1 to (times - 1)) results :+= time(name, id)(action)
    BenchmarkResult(name, id, results.flatMap(r => r.measures))
  }
}

