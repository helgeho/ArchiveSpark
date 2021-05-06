package org.archive.archivespark.sparkling.util

import java.util.concurrent.{CancellationException, FutureTask}

import org.archive.archivespark.sparkling.logging.{Log, LogContext}

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

object IteratorUtil {
  implicit val logContext: LogContext = LogContext(this)

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
            dropWhile(lookup) { _ <= itemMapped }
            Iterator(item)
          } else {
            if (end) { if (lookup.hasNext) Iterator(item) else Iterator.empty }
            else { if (lookup.head > itemMapped && lookup.head < nextMapped) Iterator(item) else Iterator.empty }
          }
        }
        else None
      }.flatten
    } else Iterator.empty
  }

  def adjacent[A](sortedElement: TraversableOnce[A], sortedLookup: TraversableOnce[A])(implicit evidence: A => Ordered[A]): Iterator[A] =
    adjacent[A, A](sortedElement, sortedLookup, identity)(evidence)

  def groupSorted[A](sorted: TraversableOnce[A]): Iterator[(A, Iterator[A])] = groupSorted[A, A](sorted, identity)

  def groupSortedBy[A, B](sorted: TraversableOnce[A])(groupBy: A => B): Iterator[(B, Iterator[A])] = groupSorted(sorted, groupBy)

  def groupSorted[A, B](sorted: TraversableOnce[A], groupBy: A => B): Iterator[(B, Iterator[A])] = {
    val buffered = sorted.toIterator.buffered
    var prevGroup: Iterator[A] = Iterator.empty
    whileDefined {
      consume(prevGroup)
      if (buffered.hasNext) Some {
        val group = groupBy(buffered.head)
        prevGroup = whileDefined { if (buffered.hasNext && groupBy(buffered.head) == group) Some(buffered.next) else None }
        (group, prevGroup)
      }
      else None
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
      }
      else None
    }
  }

  def groupedGroups[A](groups: TraversableOnce[TraversableOnce[A]], max: Long)(length: A => Long): Iterator[Iterator[A]] = {
    val iter = groups.toIterator
    var prev: Iterator[A] = Iterator.empty
    whileDefined {
      consume(prev)
      var size = 0L
      if (iter.hasNext) Some {
        prev = whileDefined {
          if (size < max && iter.hasNext) Some { touch(iter.next.toIterator)(size += length(_)) }
          else None
        }.flatten
        prev
      }
      else None
    }
  }

  def groupedN[A](iter: TraversableOnce[A], n: Long): Iterator[Iterator[A]] = grouped(iter, n)(_ => 1L)

  def consume(iter: Iterator[_]): Unit = while (iter.hasNext) iter.next()

  def takeWhile[A](iter: BufferedIterator[A], preAdvance: Boolean = false)(condition: A => Boolean): Iterator[A] = whileDefined {
    if (preAdvance && iter.hasNext) iter.next()
    if (iter.hasNext && condition(iter.head)) Some(if (preAdvance) iter.head else iter.next()) else None
  }

  def takeUntil[A](iter: BufferedIterator[A], preAdvance: Boolean = false, including: Boolean = false)(condition: A => Boolean): Iterator[A] = {
    val taken = takeWhile(iter, preAdvance)(!condition(_))
    if (including) taken ++ lazyIter(if (iter.hasNext) Iterator(iter.next()) else Iterator.empty) else taken
  }

  def dropWhile[A](iter: BufferedIterator[A])(condition: A => Boolean): BufferedIterator[A] = {
    while (iter.hasNext && condition(iter.head)) iter.next()
    iter
  }

  def dropUntil[A](iter: BufferedIterator[A], including: Boolean = false)(condition: A => Boolean): BufferedIterator[A] = {
    dropWhile(iter)(!condition(_))
    if (including && iter.hasNext) dropWhile(iter)(condition(_))
    iter
  }

  def dropButLast[A](iter: BufferedIterator[A])(condition: A => Boolean): Iterator[A] = last(takeWhile(iter)(condition)).toIterator ++ iter

  def getLazy[A](items: (Int => A)*): Iterator[A] = (0 until items.size).toIterator.map(i => items(i)(i))

  def catLazy[A](items: (Int => TraversableOnce[A])*): Iterator[A] = getLazy(items: _*).flatten

  def lazyIter[A](iter: => TraversableOnce[A]): Iterator[A] = catLazy(_ => iter)

  def lazyFlatMap[A, B](iter: => TraversableOnce[A])(map: A => TraversableOnce[B]): Iterator[B] = iter.flatMap(r => lazyIter(map(r))).toIterator

  def empty[A](op: => Unit): Iterator[A] = noop(op)
  def noop[A](op: => Unit): Iterator[A] = lazyIter {
    op
    Iterator.empty
  }

  // TODO: integrate Spark's own InteruptibleIterator
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
      current.map(b => (a, b))
    } ++ noop { if (current.isDefined) cleanup(current.get) }
  }

  def zipLazy[A, B](iter: Iterator[A])(zip: A => Option[ManagedVal[B]], throwOnClearError: Boolean = false): Iterator[(A, ManagedVal[B])] = { zipCleanup(iter)(zip)(_.clear(throwOnClearError)) }

  def zipNext[A](iter: Iterator[A]): Iterator[(A, Option[A])] = {
    val (a, b) = iter.duplicate
    if (b.hasNext) b.next()
    a.map { (_, if (b.hasNext) Some(b.next()) else None) }
  }

  def firstOf[A, B](ordered: Iterator[A])(of: A => B): Iterator[A] = {
    var prev: Option[B] = None
    ordered.flatMap { item =>
      val key = of(item)
      if (prev.contains(key)) None
      else {
        prev = Some(key)
        Some(item)
      }
    }
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

  def last[A](iter: Iterator[A]): Option[A] =
    if (!iter.hasNext) None
    else Some {
      var last = iter.next
      while (iter.hasNext) last = iter.next
      last
    }

  def touch[A](iter: Iterator[A])(touch: A => Unit): Iterator[A] = iter.map { a =>
    touch(a)
    a
  }

  def count(iter: TraversableOnce[_]): Long = iter.map(_ => 1L).sum

  def drop[A](iter: Iterator[A], n: Long): Iterator[A] = {
    for (i <- 1L to n) if (iter.hasNext) iter.next()
    iter
  }

  def take[A](iter: Iterator[A], n: Long): Iterator[A] = whileDefined((1L to n).toIterator.map { _ => if (iter.hasNext) Some(iter.next()) else None })

  def zipWithIndex[A](iter: Iterator[A]): Iterator[(A, Long)] = {
    var idx = -1L
    iter.map { item =>
      idx += 1
      (item, idx)
    }
  }

  @tailrec
  def treeReduce[A](iter: Iterator[A])(reduce: (A, A) => A): Option[A] = {
    if (iter.hasNext) {
      val head = iter.next
      if (iter.hasNext) treeReduce((Iterator(head) ++ iter).grouped(2).map { seq => if (seq.size == 2) reduce(seq.head, seq(1)) else seq.head })(reduce) else Some(head)
    } else None
  }

  def preload[A, B](iter: Iterator[A], numPreload: Int = 0, parallelism: Int = 2)(preload: A => B): Iterator[B] = {
    parallelMap[A, B](iter, parallelism = parallelism, maxBuffer = numPreload, skipNotKill = false, cancelNotWait = false)(preload).map(_.get)
  }

  def parallelMapKill[A, B](iter: Iterator[A], parallelism: Int = 5, maxBuffer: Int = 100)(map: A => B): Iterator[B] = {
    parallelMap(iter, parallelism, maxBuffer, skipNotKill = false, cancelNotWait = true)(map).map(_.get)
  }

  def parallelMap[A, B](iter: Iterator[A], parallelism: Int = 5, maxBuffer: Int = 100, skipNotKill: Boolean = true, cancelNotWait: Boolean = true)(map: A => B): Iterator[Option[B]] = {
    class FutureVal(f: Future[Boolean], value: FutureVal => Option[B]) {
      val future: Future[Option[B]] = ConcurrencyUtil.mapFuture(f)(_ => value(this))
      var task: Option[FutureTask[B]] = None
    }

    val buffer = collection.mutable.Queue.empty[FutureVal]

    def next(future: Future[Boolean]): Unit = {
      buffer.synchronized {
        if (iter.hasNext) {
          val item = iter.next
          buffer.enqueue(new FutureVal(
            future,
            { f =>
              while (maxBuffer > -1 && buffer.size > maxBuffer) {
                if (cancelNotWait) {
                  for (task <- buffer.headOption.flatMap(_.task) if !task.isDone) {
                    task.cancel(true)
                    Log.info("#parallelMap - cancelling task...")
                  }
                }
                Thread.`yield`()
              }
              val task = ConcurrencyUtil.thread(useExecutionContext = false) {
                try { map(item) }
                catch {
                  case e: java.lang.InterruptedException => if (skipNotKill) throw new RuntimeException(e) else throw e
                  case e: CancellationException          => if (skipNotKill) throw new RuntimeException(e) else throw e
                }
              }
              f.task = Some(task)
              val r =
                if (skipNotKill) {
                  try { Some(task.get) }
                  catch { case _: java.lang.InterruptedException | _: CancellationException => None }
                } else Some(task.get)
              next(future)
              r
            }
          ))
        }
      }
    }

    for (_ <- 0 to parallelism) next(ConcurrencyUtil.future(true))

    IteratorUtil.whileDefined {
      buffer.headOption.map { f =>
        try { Await.result(f.future, Duration.Inf) }
        finally { buffer.synchronized(buffer.dequeue()) }
      }
    }
  }
}
