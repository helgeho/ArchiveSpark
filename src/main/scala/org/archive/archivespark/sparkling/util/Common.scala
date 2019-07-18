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

package org.archive.archivespark.sparkling.util

import java.io.InputStream
import java.util.concurrent.{Callable, FutureTask, TimeUnit, TimeoutException}

import org.archive.archivespark.sparkling.logging.{Log, LogContext}

object Common {
  def lazyValWithCleanup[A](create: => A)(cleanup: A => Unit = (_: A) => {}): ManagedVal[A] = ManagedVal[A](create, {
    case Right(v) => cleanup(v)
    case Left(_) =>
  })

  def lazyVal[A](create: => A): ManagedVal[A] = ManagedVal[A](create, _ => {})

  def printThrow(msg: String): Nothing = {
    println(msg)
    throw new RuntimeException(msg)
  }

  def retry[R](times: Int = 30, sleepMillis: Int = 1000, log: (Int, Exception) => String)(run: Int => R)(implicit context: LogContext): R = {
    var lastException: Exception = null
    for (retry <- 0 to times) {
      if (retry > 0) Thread.sleep(sleepMillis)
      var in: InputStream = null
      try {
        return run(retry)
      } catch {
        case e: Exception =>
          Log.error(log(retry, e))
          lastException = e
      }
    }
    throw lastException
  }

  private class ProcessReporter {
    private var _time: Long = System.currentTimeMillis
    private var _status: Option[String] = None
    def lastAlive: Long = _time
    def lastStatus: Option[String] = _status
    def alive(): Unit = _time = System.currentTimeMillis
    def alive(status: String): Unit = {
      _status = Some(status)
      _time = System.currentTimeMillis
    }
  }

  def iterTimeout[R](millis: Long, iter: Iterator[R])(status: (Long, R) => String)(implicit context: LogContext): Iterator[R] = {
    iterTimeout(iter, millis, (idx: Long, item: R) => Some(status(idx, item)))
  }

  def iterTimeout[R](iter: Iterator[R], millis: Long, status: (Long, R) => Option[String] = (_: Long, _: R) => None)(implicit context: LogContext): Iterator[R] = {
    var item: Option[(R, Option[String])] = None
    var lastLog = System.currentTimeMillis
    var idx = -1L
    IteratorUtil.whileDefined {
      idx += 1
      item = timeout(millis, item.flatMap(_._2)) {
        if (iter.hasNext) Some {
          val item = iter.next()
          (item, status(idx, item))
        } else None
      }
      if (item.isDefined && System.currentTimeMillis - lastLog > millis) {
        Log.info("Process is alive..." + item.flatMap(_._2).map(status => s" - Last status: $status").getOrElse(""))
        lastLog = System.currentTimeMillis
      }
      item.map(_._1)
    }
  }

  def timeout[R](millis: Long, status: Option[String] = None)(action: => R)(implicit context: LogContext): R = {
    if (millis < 0) return action
    val future = new FutureTask[R](new Callable[R] {
      override def call(): R = action
    })
    new Thread(future).start()
    SparkUtil.cleanupTask(future, () => while (!future.isDone) future.cancel(true))
    try {
      future.get(millis, TimeUnit.MILLISECONDS)
    } catch {
      case e: TimeoutException =>
        Log.info("Timeout after " + millis + " milliseconds" + status.map(status => s" - Last status: $status").getOrElse("") + ".")
        throw e
    } finally {
      while (!future.isDone) future.cancel(true)
      SparkUtil.removeTaskCleanup(future)
    }
  }

  def timeoutWithReporter[R](millis: Long)(action: ProcessReporter => R)(implicit context: LogContext): R = {
    val reporter = new ProcessReporter
    if (millis < 0) return action(reporter)
    val future = new FutureTask[R](new Callable[R] {
      override def call(): R = action(reporter)
    })
    new Thread(future).start()
    SparkUtil.cleanupTask(future, () => while (!future.isDone) future.cancel(true))
    var lastAlive = reporter.lastAlive
    while (true) {
      try {
        val wait = lastAlive + millis - System.currentTimeMillis
        val result = future.get(if (wait <= 0) 1 else wait, TimeUnit.MILLISECONDS)
        SparkUtil.removeTaskCleanup(future)
        return result
      } catch {
        case e: TimeoutException =>
          if (lastAlive == reporter.lastAlive) {
            Log.info("Timeout after " + millis + " milliseconds" + reporter.lastStatus.map(status => s" - Last status: $status").getOrElse("") + ".")
            while (!future.isDone) future.cancel(true)
            SparkUtil.removeTaskCleanup(future)
            throw e
          } else {
            Log.info("Process is alive..." + reporter.lastStatus.map(status => s" - Last status: $status").getOrElse(""))
            lastAlive = reporter.lastAlive
          }
        case _: Exception => SparkUtil.removeTaskCleanup(future)
      }
    }
    throw new RuntimeException()
  }

  def touch[A](a: A)(touch: A => Unit): A = {
    touch(a)
    a
  }
}
