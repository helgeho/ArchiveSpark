package org.archive.archivespark.sparkling.io

import org.archive.archivespark.sparkling.util.{CleanupIterator, IteratorUtil}

object HdfsBackedMapOperations {
  class HdfsBackedSubtractKeysMap private[io] (source: PrimitiveHdfsBackedMap, subtract: Seq[PrimitiveHdfsBackedMap]) extends PrimitiveHdfsBackedMap {
    private def filter[A](iter: CleanupIterator[A], subtract: Seq[CleanupIterator[A]], getKey: A => String): CleanupIterator[A] = {
      val subtractIters = subtract
      IteratorUtil.cleanup(
        iter.filter { item =>
          val k = getKey(item)
          subtractIters.foreach(IteratorUtil.dropWhile(_)(getKey(_) < k))
          !subtractIters.exists(iter => iter.hasNext && getKey(iter.head) == k)
        },
        () => {
          iter.clear()
          subtract.foreach(_.clear())
        }
      )
    }

    override def key: String => String = source.key

    override def get(key: String): Option[CleanupIterator[String]] = {
      filter[CleanupIterator[String]](CleanupIterator(source.get(key).toIterator), subtract.map(s => CleanupIterator(s.get(key).toIterator)), _ => key).toSeq.headOption
    }

    override def cache: Boolean = source.cache

    override def preloadLength: Boolean = source.preloadLength

    override def iter: CleanupIterator[(String, Iterator[String])] = { filter[(String, Iterator[String])](source.iter, subtract.map(_.iter), _._1) }

    override def from(key: String, lookupPrefixes: Boolean = false): CleanupIterator[(String, Iterator[String])] = {
      filter[(String, Iterator[String])](source.from(key, lookupPrefixes), subtract.map(_.from(key, lookupPrefixes)), _._1)
    }

    override def before(key: String): CleanupIterator[(String, Iterator[String])] = { filter[(String, Iterator[String])](source.before(key), subtract.map(_.before(key)), _._1) }

    override def fromWithIndex(key: String, lookupPrefixes: Boolean = false): CleanupIterator[((String, Iterator[String]), Int)] = {
      filter[((String, Iterator[String]), Int)](source.fromWithIndex(key, lookupPrefixes), subtract.map(_.fromWithIndex(key, lookupPrefixes)), _._1._1)
    }

    override def dropKeys(n: Int): CleanupIterator[(String, Iterator[String])] = {
      val sourceIter = source.dropKeys(n)
      if (sourceIter.hasNext) {
        val key = sourceIter.head._1
        filter[(String, Iterator[String])](sourceIter, subtract.map(_.fromWithIndex(key).chain(_.map(_._1))), _._1)
      } else CleanupIterator.empty
    }
  }

  class HdfsBackedSubtractMap[V] private[io] (source: PrimitiveHdfsBackedMap, subtract: Seq[PrimitiveHdfsBackedMap]) extends PrimitiveHdfsBackedMap {
    private def filter[A](
        iter: CleanupIterator[A],
        subtract: Seq[CleanupIterator[A]],
        getKey: A => String,
        getValues: A => Iterator[String],
        integrate: (A, Iterator[String]) => A
    ): CleanupIterator[A] = {
      val subtractIters = subtract
      IteratorUtil.cleanup(
        iter.flatMap { item =>
          val k = getKey(item)
          val v = getValues(item).toSet -- subtractIters.flatMap { iter =>
            IteratorUtil.dropWhile(iter)(getKey(_) < k)
            if (iter.hasNext && getKey(iter.head) == k) getValues(iter.head) else Seq.empty
          }.toSet
          if (v.isEmpty) None else Some(integrate(item, v.toSeq.sorted.toIterator))
        },
        () => {
          iter.clear()
          subtract.foreach(_.clear())
        }
      )
    }

    override def key: String => String = source.key

    override def get(key: String): Option[CleanupIterator[String]] = {
      filter[CleanupIterator[String]](CleanupIterator(source.get(key)), subtract.map(s => CleanupIterator(s.get(key))), _ => key, identity, (_, v) => CleanupIterator(v)).toSeq.headOption
    }

    override def cache: Boolean = source.cache

    override def preloadLength: Boolean = source.preloadLength

    override def iter: CleanupIterator[(String, Iterator[String])] = { filter[(String, Iterator[String])](source.iter, subtract.map(_.iter), _._1, _._2, { case ((k, _), v) => k -> v }) }

    override def from(key: String, lookupPrefixes: Boolean = false): CleanupIterator[(String, Iterator[String])] = {
      filter[(String, Iterator[String])](source.from(key, lookupPrefixes), subtract.map(_.from(key, lookupPrefixes)), _._1, _._2, { case ((k, _), v) => k -> v })
    }

    override def before(key: String): CleanupIterator[(String, Iterator[String])] = {
      filter[(String, Iterator[String])](source.before(key), subtract.map(_.before(key)), _._1, _._2, { case ((k, _), v) => k -> v })
    }

    override def fromWithIndex(key: String, lookupPrefixes: Boolean = false): CleanupIterator[((String, Iterator[String]), Int)] = {
      filter[((String, Iterator[String]), Int)](
        source.fromWithIndex(key, lookupPrefixes),
        subtract.map(_.fromWithIndex(key, lookupPrefixes)),
        _._1._1,
        _._1._2,
        { case (((k, _), i), v) => (k -> v, i) }
      )
    }

    override def dropKeys(n: Int): CleanupIterator[(String, Iterator[String])] = {
      val sourceIter = source.dropKeys(n)
      if (sourceIter.hasNext) {
        val key = sourceIter.head._1
        filter[(String, Iterator[String])](sourceIter, subtract.map(_.fromWithIndex(key).chain(_.map(_._1))), _._1, _._2, { case ((k, _), v) => k -> v })
      } else CleanupIterator.empty
    }
  }
}
