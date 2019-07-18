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

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success

object IteratorUtil {
  def adjacent[A, B](sorted: TraversableOnce[A], sortedLookup: TraversableOnce[B], map: A => B)(implicit evidence: B => Ordered[B]): Iterator[A] = {
    val items = sorted.toIterator
    val lookup = sortedLookup.toIterator.buffered
    if (items.hasNext) {
      var next = items.next()
      var nextMapped = map(next)
      var end = false
      whileDefined {
        if (!end && lookup.hasNext) Some {
          val item = next
          val itemMapped = nextMapped
          if (items.hasNext) {
            next = items.next()
            nextMapped = map(next)
          } else end = true
          if (lookup.head <= itemMapped) {
            dropWhile(lookup) {_ <= itemMapped}
            Iterator(item)
          } else {
            if (end) {
              if (lookup.hasNext) Iterator(item)
              else Iterator.empty
            } else {
              if (lookup.head > itemMapped && lookup.head < nextMapped) Iterator(item)
              else Iterator.empty
            }
          }
        } else None
      }.flatten
    } else Iterator.empty
  }

  def adjacent[A](sortedElement: TraversableOnce[A], sortedLookup: TraversableOnce[A])(implicit evidence: A => Ordered[A]): Iterator[A] = adjacent[A, A](sortedElement, sortedLookup, identity)(evidence)

  def groupSorted[A](sorted: TraversableOnce[A]): Iterator[(A, Iterator[A])] = groupSorted[A, A](sorted, identity)

  def groupSortedBy[A, B](sorted: TraversableOnce[A])(groupBy: A => B): Iterator[(B, Iterator[A])] = groupSorted(sorted, groupBy)

  def groupSorted[A, B](sorted: TraversableOnce[A], groupBy: A => B): Iterator[(B, Iterator[A])] = {
    val buffered = sorted.toIterator.buffered
    var prevGroup: Iterator[A] = Iterator.empty
    whileDefined {
      consume(prevGroup)
      if (buffered.hasNext) Some {
        val group = groupBy(buffered.head)
        prevGroup = whileDefined {
          if (buffered.hasNext && groupBy(buffered.head) == group) Some(buffered.next)
          else None
        }
        (group, prevGroup)
      } else None
    }
  }

  def takeMax[A](iter: BufferedIterator[A], max: Long, over: Boolean = false, firstOver: Boolean = false)(length: A => Long): Iterator[A] = {
    var size = 0L
    whileDefined {
      if (iter.hasNext) {
        val nextLength = length(iter.head)
        if ((firstOver && size == 0) || (over && size < max) || (!over && size + nextLength <= max)) {
          size += nextLength
          Some(iter.next)
        } else None
      } else None
    }
  }

  def grouped[A](iter: TraversableOnce[A], max: Long, over: Boolean = false)(length: A => Long): Iterator[Iterator[A]] = {
    val buffered = iter.toIterator.buffered
    var group: Iterator[A] = Iterator.empty
    whileDefined {
      consume(group)
      if (buffered.hasNext) Some {
        group = takeMax(buffered, max, over, firstOver = true)(length)
        group
      } else None
    }
  }

  def groupedN[A](iter: TraversableOnce[A], n: Long): Iterator[Iterator[A]] = grouped(iter, n)(_ => 1L)

  def consume(iter: Iterator[_]): Unit = while (iter.hasNext) iter.next()

  def takeWhile[A](iter: BufferedIterator[A], preAdvance: Boolean = false)(condition: A => Boolean): Iterator[A] = whileDefined {
    if (preAdvance && iter.hasNext) iter.next()
    if (iter.hasNext && condition(iter.head)) Some(if (preAdvance) iter.head else iter.next())
    else None
  }

  def takeUntil[A](iter: BufferedIterator[A], preAdvance: Boolean = false, including: Boolean = false)(condition: A => Boolean): Iterator[A] = {
    val taken = takeWhile(iter, preAdvance)(!condition(_))
    if (including) taken ++ lazyIter(if (iter.hasNext) Iterator(iter.next()) else Iterator.empty)
    else taken
  }

  def dropWhile[A](iter: BufferedIterator[A])(condition: A => Boolean): BufferedIterator[A] = {
    while (iter.hasNext && condition(iter.head)) iter.next()
    iter
  }

  def dropUntil[A](iter: BufferedIterator[A], including: Boolean = false)(condition: A => Boolean): BufferedIterator[A] = {
    dropWhile(iter)(!condition(_))
    if (including && iter.hasNext) iter.next()
    iter
  }

  def getLazy[A](items: (Int => A)*): Iterator[A] = (0 until items.size).toIterator.map(i => items(i)(i))

  def catLazy[A](items: (Int => TraversableOnce[A])*): Iterator[A] = getLazy(items: _*).flatten

  def lazyIter[A](iter: => TraversableOnce[A]): Iterator[A] = catLazy(_ => iter)

  def lazyFlatMap[A, B](iter: => TraversableOnce[A])(map: A => TraversableOnce[B]): Iterator[B] = iter.flatMap(r => lazyIter(map(r))).toIterator

  def empty[A](op: => Unit): Iterator[A] = noop(op)
  def noop[A](op: => Unit): Iterator[A] = lazyIter {
    op
    Iterator.empty
  }

  def cleanup[A](iter: Iterator[A], cleanup: () => Unit): CleanupIterator[A] = CleanupIterator(iter, cleanup)

  def untilUndefined[A](iter: Iterator[Option[A]]): Iterator[A] = whileDefined(iter)
  def whileDefined[A](iter: Iterator[Option[A]]): Iterator[A] = iter.takeWhile(_.isDefined).map(_.get)

  def untilUndefined[A](item: => Option[A]): Iterator[A] = whileDefined(item)
  def whileDefined[A](item: => Option[A]): Iterator[A] = whileDefined(Iterator.continually(item))

  def zipCleanup[A, B](iter: Iterator[A])(zip: A => Option[B])(cleanup: B => Unit): Iterator[(A, B)] = {
    var current: Option[B] = None
    iter.flatMap { a =>
      if (current.isDefined) cleanup(current.get)
      current = zip(a)
      current.map(b => (a,b))
    } ++ noop {
      if (current.isDefined) cleanup(current.get)
    }
  }

  def zipLazy[A, B](iter: Iterator[A])(zip: A => Option[ManagedVal[B]], throwOnClearError: Boolean = false): Iterator[(A, ManagedVal[B])] = {
    zipCleanup(iter)(zip)(_.clear(throwOnClearError))
  }

  def zipNext[A](iter: Iterator[A]): Iterator[(A, Option[A])] = {
    val (a, b) = iter.duplicate
    if (b.hasNext) b.next()
    a.map{(_, if (b.hasNext) Some(b.next()) else None)}
  }

  def distinctOrdered[A](ordered: Iterator[A]): Iterator[A] = {
    var prev: Option[A] = None
    ordered.flatMap { item =>
      if (prev.contains(item)) None
      else {
        prev = Some(item)
        prev
      }
    }
  }

  def distinct[A](iter: Iterator[A]): Iterator[A] = {
    val items = collection.mutable.Set.empty[A]
    iter.filter { item =>
      if (items.contains(item)) false
      else {
        items.add(item)
        true
      }
    }
  }

  def last[A](iter: Iterator[A]): Option[A] = if (!iter.hasNext) None else Some {
    var last = iter.next
    while (iter.hasNext) last = iter.next
    last
  }

  def count(iter: TraversableOnce[_]): Long = iter.map(_ => 1L).sum

  def drop[A](iter: Iterator[A], n: Long): Iterator[A] = {
    for (i <- 1L to n) if (iter.hasNext) iter.next()
    iter
  }

  def take[A](iter: Iterator[A], n: Long): Iterator[A] = whileDefined((1L to n).toIterator.map { _ =>
    if (iter.hasNext) Some(iter.next())
    else None
  })

  def zipWithIndex[A](iter: Iterator[A]): Iterator[(A, Long)] = {
    var idx = -1L
    iter.map { item =>
      idx += 1
      (item, idx)
    }
  }

  def preload[A, B](iter: Iterator[A])(preload: A => B): Iterator[B] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if (!iter.hasNext) return Iterator.empty
    val (promises, unresolved) = iter.map(item => (item, Promise[B]())).duplicate
    val buffered = unresolved.buffered
    def resolveNext(item: A): Unit = Future {
      preload(item)
    }.andThen { case Success(value) =>
      buffered.next()._2.success(value)
      if (buffered.hasNext) resolveNext(buffered.head._1)
    }
    resolveNext(buffered.head._1)
    whileDefined {
      if (promises.hasNext) Some {
        Await.result(promises.next()._2.future, Duration.Inf)
      } else None
    }
  }
}
