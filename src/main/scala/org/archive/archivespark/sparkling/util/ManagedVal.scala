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

package org.archive.archivespark.sparkling.util

import scala.util.Try

class ManagedVal[A] private(create: => A, cleanup: Either[Exception, A] => Unit = (_: Either[Exception, A]) => {}, val lazyEval: Boolean = true) {
  private var value: Option[Either[Exception, A]] = None

  def evaluated: Boolean = value.isDefined

  def eval(create: => A = create): Either[Exception, A] = value.getOrElse(reeval(create))

  def reeval(create: => A = create): Either[Exception, A] = {
    clear(false)
    value = Some {
      try {
        val right = Right(create)
        SparkUtil.cleanupTask(this, () => clear(false))
        right
      } catch {
        case e: Exception =>
          val left = Left(e)
          Try(cleanup(left))
          left
      }
    }
    value.get
  }

  def retry(create: => A = create): Option[Either[Exception, A]] = if (value.isDefined && value.get.isRight) None else Some(reeval(create))

  def either: Either[Exception, A] = eval()

  def get: A = either match {
    case Right(v) => v
    case Left(e) => throw e
  }

  def option: Option[A] = either.right.toOption

  def clear(throwOnError: Boolean = true): Unit = if (value.isDefined && value.get.isRight) try {
    val either = value.get
    value = None
    cleanup(either)
  } catch {
    case e: Exception => if (throwOnError) throw e
  } finally {
    SparkUtil.removeTaskCleanup(this)
  }

  def apply[R](action: A => R, throwOnClearError: Boolean = false): R = try {
    action(get)
  } finally {
    clear(throwOnClearError)
  }

  def map[B](map: A => B, cleanup: Either[Exception, B] => Unit = (_: Either[Exception, B]) => {}, bubbleClear: Boolean = true, lazyEval: Boolean = true): ManagedVal[B] = ManagedVal({
    either match {
      case Right(v) => map(v)
      case Left(e) => throw e
    }
  }, { either =>
    cleanup(either)
    if (bubbleClear) clear()
  }, lazyEval = lazyEval)

  def iter[B](iter: A => Iterator[B], cleanup: () => Unit = () => {}, lazyEval: Boolean = true, throwOnError: Boolean = false): ManagedVal[CleanupIterator[B]] = {
    var wrapped: Option[ManagedVal[CleanupIterator[B]]] = None
    wrapped = Some(map({ a =>
      CleanupIterator(iter(a), () => {
        cleanup()
        wrapped.get.clear()
      }, throwOnError = throwOnError)
    }, lazyEval = true))
    if (!lazyEval) wrapped.get.eval()
    wrapped.get
  }

  if (!lazyEval) eval()
}

object ManagedVal {
  def apply[A](create: => A, cleanup: Either[Exception, A] => Unit = (_: Either[Exception, A]) => {}, lazyEval: Boolean = true): ManagedVal[A] = new ManagedVal[A](create, cleanup, lazyEval)
}