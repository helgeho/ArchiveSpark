package org.archive.archivespark.sparkling.io

import java.io.InputStream

import org.archive.archivespark.sparkling.Sparkling.prop
import org.archive.archivespark.sparkling.util.{CleanupIterator, Common, IteratorUtil, ManagedVal, ValueSupplier}

object HdfsBackedMap {
  var minCacheSize: Int = prop(100)(minCacheSize, minCacheSize = _)
  var maxCacheSize: Int = prop(300)(maxCacheSize, maxCacheSize = _)
  private val cache = collection.mutable.Map.empty[(String, String), ManagedVal[ValueSupplier[InputStream]]]
  private var cached = List.empty[(String, String)]

  def cache(key: String, file: String, lines: ManagedVal[ValueSupplier[InputStream]]): CleanupIterator[String] = cache.synchronized {
    if (cache.size >= maxCacheSize) {
      val (keep, drop) = cached.splitAt(minCacheSize)
      cache --= drop
      cached = keep
    }
    cache.update((key, file), lines)
    cached +:= (key, file)
    val in = lines.get.get
    IteratorUtil.cleanup(IOUtil.lines(in), in.close)
  }

  def cached(key: String, file: String): Option[CleanupIterator[String]] = cache.get((key, file)).map(_.get.get).map(in => IteratorUtil.cleanup(IOUtil.lines(in), in.close))

  def apply(path: String): HdfsBackedMap[String] = apply[String](path, identity, identity)
  def apply(path: String, cache: Boolean): HdfsBackedMap[String] = apply[String](path, identity, identity, cache)
  def apply(path: String, cache: Boolean, preloadLength: Boolean): HdfsBackedMap[String] = apply[String](path, identity, identity, preloadLength)
  def apply(path: String, key: String => String): HdfsBackedMap[String] = apply[String](path, key, identity)
  def apply(path: String, key: String => String, cache: Boolean): HdfsBackedMap[String] = apply[String](path, key, identity, cache)
  def apply(path: String, key: String => String, cache: Boolean, preloadLength: Boolean): HdfsBackedMap[String] = apply[String](path, key, identity, preloadLength)
  def apply[V](path: String, key: String => String, value: String => V, cache: Boolean = true, preloadLength: Boolean = true, groupFiles: Int = 1): HdfsBackedMap[V] =
    new HdfsBackedMap[V](new PrimitiveHdfsBackedMapImpl(path, key, cache, preloadLength, groupFiles), value)
}

class HdfsBackedMap[V] private (primitive: PrimitiveHdfsBackedMap, value: String => V) extends Serializable {
  def first(key: String): Option[V] = get(key).filter(_.hasNext).map(_.next())

  def get(key: String): Option[CleanupIterator[V]] = primitive.get(key).map(_.chain(_.map(value)))

  def iter: CleanupIterator[(String, ManagedVal[Iterator[V]])] = primitive.iter.chain(_.map { case (k, v) => k -> Common.lazyVal(v.map(value)) })

  def from(key: String, lookupPrefixes: Boolean = false): CleanupIterator[(String, ManagedVal[Iterator[V]])] = primitive.from(key, lookupPrefixes).chain(_.map { case (k, v) =>
    k -> Common.lazyVal(v.map(value))
  })

  def before(key: String): CleanupIterator[(String, ManagedVal[Iterator[V]])] = primitive.before(key).chain(_.map { case (k, v) => k -> Common.lazyVal(v.map(value)) })

  def fromWithIndex(key: String, lookupPrefixes: Boolean = false): CleanupIterator[((String, ManagedVal[Iterator[V]]), Int)] = {
    primitive.fromWithIndex(key, lookupPrefixes).chain(_.map { case ((k, v), i) => (k -> Common.lazyVal(v.map(value)), i) })
  }

  def dropKeys(n: Int): CleanupIterator[(String, ManagedVal[Iterator[V]])] = { primitive.dropKeys(n).chain(_.map { case (k, v) => k -> Common.lazyVal(v.map(value)) }) }

  def subtractKeys(paths: Set[String]): HdfsBackedMap[V] = new HdfsBackedMap[V](primitive.subtractKeys(paths), value)

  def subtract(paths: Set[String]): HdfsBackedMap[V] = new HdfsBackedMap[V](primitive.subtract(paths), value)
}
