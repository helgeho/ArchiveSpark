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

  override def headOption: Option[A] = if (hasNext) Some(head) else None

  override def head: A = headValue match {
    case Some(v) => v
    case None =>
      headValue = Some(next())
      headValue.get
  }

  def clear(throwOnError: Boolean = throwOnError): Unit = {
    if (!cleanedUp)
      try { cleanup() }
      catch { case e: Exception => if (throwOnError) throw e }
      finally {
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

  def chainOpt[B](action: CleanupIterator[A] => Option[Iterator[B]]): Option[CleanupIterator[B]] = action(this) match {
    case Some(iter) => Some(CleanupIterator[B](iter, () => clear(), throwOnError))
    case None =>
      clear()
      None
  }

  def onClear(cleanup: () => Unit, throwOnError: Boolean = false): CleanupIterator[A] = CleanupIterator(
    this,
    () => {
      cleanup()
      clear()
    },
    throwOnError
  )

//  override def finalize(): Unit = {
//    clear(throwOnError = false)
//    super.finalize()
//  }
}

object CleanupIterator {
  def apply[A](iter: Iterator[A], cleanup: () => Unit, throwOnError: Boolean = false): CleanupIterator[A] = {
    val cleanupIter = new CleanupIterator(iter, cleanup, throwOnError)
    SparkUtil.cleanupTask(cleanupIter, () => cleanupIter.clear(false))
    cleanupIter
  }

  def apply[A](traversable: TraversableOnce[A]): CleanupIterator[A] = apply(traversable.toIterator, () => {})

  def empty[A]: CleanupIterator[A] = apply(Iterator.empty)

  def lazyIter[A](iter: => CleanupIterator[A]): CleanupIterator[A] = flatten(Iterator(true).map(_ => iter))

  def combine[A](iters: CleanupIterator[A]*): CleanupIterator[A] = flatten(iters.toIterator).onClear(() => iters.foreach(_.clear()))

  def flatten[A](iters: Iterator[CleanupIterator[A]]): CleanupIterator[A] = {
    var currentIter: Option[CleanupIterator[A]] = None
    CleanupIterator(
      iters.flatMap { iter =>
        currentIter = Some(iter)
        iter
      },
      () => { for (iter <- currentIter) iter.clear() }
    )
  }
}
