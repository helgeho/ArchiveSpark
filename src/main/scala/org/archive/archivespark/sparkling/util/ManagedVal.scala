package org.archive.archivespark.sparkling.util

import scala.util.Try

class ManagedVal[A] private (create: => A, cleanup: Either[Exception, A] => Unit = (_: Either[Exception, A]) => {}, val lazyEval: Boolean = true) {
  private var locked = 0
  private var clearOnRelease = false
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
    case Left(e)  => throw e
  }

  def option: Option[A] = either.right.toOption

  def clear(throwOnError: Boolean = true): Unit = if (value.isDefined && value.get.isRight)
    try {
      val either = value.get
      value = None
      cleanup(either)
    } catch { case e: Exception => if (throwOnError) throw e }
    finally { SparkUtil.removeTaskCleanup(this) }

  def apply[R](action: A => R, throwOnClearError: Boolean = false): R =
    try { action(get) }
    finally { clear(throwOnClearError) }

  def map[B](map: A => B, cleanup: Either[Exception, B] => Unit = (_: Either[Exception, B]) => {}, bubbleClear: Boolean = true, lazyEval: Boolean = true): ManagedVal[B] = ManagedVal(
    {
      either match {
        case Right(v) => map(v)
        case Left(e)  => throw e
      }
    },
    { either =>
      cleanup(either)
      if (bubbleClear) clear()
    },
    lazyEval = lazyEval
  )

  def iter[B](iter: A => Iterator[B], cleanup: () => Unit = () => {}, lazyEval: Boolean = true, throwOnError: Boolean = false): ManagedVal[CleanupIterator[B]] = {
    var wrapped: Option[ManagedVal[CleanupIterator[B]]] = None
    wrapped = Some(map(
      { a =>
        CleanupIterator(
          iter(a),
          () => {
            if (wrapped.isDefined) {
              cleanup()
              wrapped.get.clear()
              wrapped = None
            }
          },
          throwOnError = throwOnError
        )
      },
      lazyEval = true
    ))
    if (!lazyEval) wrapped.get.eval()
    wrapped.get
  }

//  override def finalize(): Unit = {
//    clear(throwOnError = false)
//    super.finalize()
//  }

  if (!lazyEval) eval()
}

object ManagedVal {
  def apply[A](create: => A, cleanup: Either[Exception, A] => Unit = (_: Either[Exception, A]) => {}, lazyEval: Boolean = true): ManagedVal[A] = new ManagedVal[A](create, cleanup, lazyEval)
}
