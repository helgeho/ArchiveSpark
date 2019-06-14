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

class CleanupIterator[A] private (iter: Iterator[A], cleanup: () => Unit, throwOnError: Boolean) extends BufferedIterator[A] {
  private var cleanedUp = false
  private var headValue: Option[A] = None

  override def hasNext: Boolean = {
    if (cleanedUp) false
    else if (headValue.isDefined || iter.hasNext) true
    else {
      clear(throwOnError)
      false
    }
  }

  override def next(): A = headValue match {
    case Some(v) =>
      headValue = None
      v
    case None => iter.next()
  }

  override def head: A = headValue match {
    case Some(v) => v
    case None =>
      headValue = Some(next())
      headValue.get
  }

  def clear(throwOnError: Boolean = throwOnError): Unit = {
    if (!cleanedUp) try {
      cleanup()
    } catch {
      case e: Exception => if (throwOnError) throw e
    } finally {
      headValue = None
      cleanedUp = true
      SparkUtil.removeTaskCleanup(this)
    }
  }

  def iter[R](action: CleanupIterator[A] => R, throwOnError: Boolean): R = {
    val r = action(this)
    clear(throwOnError)
    r
  }

  def iter[R](action: CleanupIterator[A] => R): R = iter(action, throwOnError)

  def chain[B](action: CleanupIterator[A] => Iterator[B]): CleanupIterator[B] = CleanupIterator[B](action(this), () => clear(), throwOnError)
}

object CleanupIterator {
  def apply[A](iter: Iterator[A], cleanup: () => Unit, throwOnError: Boolean = false): CleanupIterator[A] = {
    val cleanupIter = new CleanupIterator(iter, cleanup, throwOnError)
    SparkUtil.cleanupTask(cleanupIter, () => cleanupIter.clear(false))
    cleanupIter
  }

  def apply[A](traversable: TraversableOnce[A]): CleanupIterator[A] = apply(traversable.toIterator, () => {})

  def empty[A]: CleanupIterator[A] = apply(Iterator.empty)

  def flatMap[A](iters: Iterator[CleanupIterator[A]]): CleanupIterator[A] = {
    var currentIter: Option[CleanupIterator[A]] = None
    CleanupIterator(iters.flatMap { iter =>
      currentIter = Some(iter)
      currentIter.get
    }, () =>{
      if (currentIter.isDefined) currentIter.get.clear()
    })
  }
}