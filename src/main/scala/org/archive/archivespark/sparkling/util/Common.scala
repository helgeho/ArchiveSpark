package org.archive.archivespark.sparkling.util

import java.util.concurrent.{Callable, FutureTask, TimeUnit, TimeoutException}

import org.archive.archivespark.sparkling.Sparkling
import org.archive.archivespark.sparkling.logging.{Log, LogContext}

import scala.util.Try

object Common {
  def lazyValWithCleanup[A](create: => A)(cleanup: A => Unit = (_: A) => {}): ManagedVal[A] = ManagedVal[A](
    create,
    {
      case Right(v) => cleanup(v)
      case Left(_)  =>
    }
  )

  def lazyVal[A](create: => A): ManagedVal[A] = ManagedVal[A](create, lazyEval = true)

  def cleanup[A](action: => A, catchCloseException: Boolean = true)(cleanup: () => Unit): A =
    try { action }
    finally { if (catchCloseException) Try(cleanup()) else cleanup() }

  def printThrow(msg: String): Nothing = {
    println(msg)
    throw new RuntimeException(msg)
  }

  def retry[R](times: Int = 30, sleepMillis: Int = 1000, log: (Int, Exception) => String)(run: Int => R)(implicit context: LogContext): R = {
    retryObj(Unit)(times, sleepMillis, log = (_, i, e) => log(i, e))((o, i) => run(i))
  }

  def retryObj[O, R](
      init: => O
  )(times: Int = 30, sleepMillis: Int = 1000, cleanup: O => Unit = (_: O) => {}, log: (Option[O], Int, Exception) => String)(run: (O, Int) => R)(implicit context: LogContext): R = {
    var lastException: Exception = null
    for (retry <- 0 to times) {
      if (retry > 0) Thread.sleep(sleepMillis)
      val obj: Option[O] = None
      try {
        val obj = Some(init)
        return run(obj.get, retry)
      } catch {
        case e: Exception =>
          for (o <- obj) Try(cleanup(o))
          Log.error(log(obj, retry, e))
          lastException = e
      }
    }
    throw lastException
  }

  private[util] class ProcessReporter {
    private var _done: Boolean = false
    private var _time: Long = System.currentTimeMillis
    private var _status: Option[String] = None
    def isDone: Boolean = _done
    def lastAlive: Long = _time
    def lastStatus: Option[String] = _status
    def alive(): Unit = _time = System.currentTimeMillis
    def alive(status: String): Unit = {
      _status = Some(status)
      _time = System.currentTimeMillis
    }
    def done(): Unit = {
      _done = true
      _time = System.currentTimeMillis
    }
    def done(status: String): Unit = {
      _done = true
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
        }
        else None
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
    val task = ConcurrencyUtil.thread(useExecutionContext = false)(action)
    try { task.get(millis, TimeUnit.MILLISECONDS) }
    catch {
      case e: TimeoutException =>
        Log.info("Timeout after " + millis + " milliseconds" + status.map(status => s" - Last status: $status").getOrElse("") + ".")
        throw e
    } finally {
      while (!task.isDone) task.cancel(true)
      SparkUtil.removeTaskCleanup(task)
    }
  }

  def timeoutOpt[R](millis: Long, status: Option[String] = None)(action: => R)(implicit context: LogContext): Option[R] = {
    try { Some(timeout(millis, status)(action)) }
    catch { case _: TimeoutException => None }
  }

  def timeoutWithReporter[R](millis: Long)(action: ProcessReporter => R)(implicit context: LogContext): R = {
    val reporter = new ProcessReporter
    if (millis < 0) return action(reporter)
    val thread = ConcurrencyUtil.thread(useExecutionContext = false)(action(reporter))
    var lastAlive = reporter.lastAlive
    while (true) {
      try {
        if (reporter.isDone) {
          val result = thread.get()
          SparkUtil.removeTaskCleanup(thread)
          return result
        } else {
          val wait = lastAlive + millis - System.currentTimeMillis
          val result = thread.get(if (wait <= 0) 1 else wait, TimeUnit.MILLISECONDS)
          SparkUtil.removeTaskCleanup(thread)
          return result
        }
      } catch {
        case e: TimeoutException =>
          if (lastAlive == reporter.lastAlive) {
            Log.info("Timeout after " + millis + " milliseconds" + reporter.lastStatus.map(status => s" - Last status: $status").getOrElse("") + ".")
            while (!thread.isDone) thread.cancel(true)
            SparkUtil.removeTaskCleanup(thread)
            throw e
          } else {
            Log.info("Process is alive..." + reporter.lastStatus.map(status => s" - Last status: $status").getOrElse(""))
            lastAlive = reporter.lastAlive
          }
        case e: Exception =>
          SparkUtil.removeTaskCleanup(thread)
          throw e
      }
    }
    throw new RuntimeException("this can/should never happen")
  }

  def touch[A](a: A)(touch: A => Unit): A = {
    touch(a)
    a
  }

  def tryOpt[A](a: => Option[A]): Option[A] = Try(a).toOption.flatten
}
