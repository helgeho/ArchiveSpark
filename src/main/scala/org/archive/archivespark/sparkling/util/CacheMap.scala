package org.archive.archivespark.sparkling.util

trait CacheMap[K,V] {
  def cached(key: K): Boolean
  def get(key: K): Option[V]
  def cache(key: K, value: V): V
  def getOrElse(key: K, value: => V): V
  def remove(key: K): Option[V]
}

class EmptyCacheMap[K,V] extends CacheMap[K,V] {
  def cached(key: K): Boolean = false
  def get(key: K): Option[V] = None
  def cache(key: K, value: V): V = value
  def getOrElse(key: K, value: => V): V = value
  def remove(key: K): Option[V] = None
}

class MinMaxCacheMap[K,V] private[util] (min: Long, max: Long, length: (K, V) => Long, onClear: Set[K] => Unit) extends CacheMap[K,V] {
  private var size: Long = 0L
  private val accesses = collection.mutable.Map.empty[K,Long]
  private val values = collection.mutable.Map.empty[K,V]

  private def access(key: K): Unit = synchronized {
    accesses(key) = accesses.getOrElse(key, 0L) + 1L
  }

  override def cached(key: K): Boolean = values.contains(key)

  override def get(key: K): Option[V] = {
    access(key)
    values.get(key)
  }

  override def cache(key: K, value: V): V = synchronized {
    val oldValue = values.remove(key)
    if (oldValue.isDefined) size -= length(key, oldValue.get)

    val thisLength = length(key, value)
    if (size != 0 && size + thisLength > max) {
      val sortedKeysWithLength = values.toList.sortBy{case (k, _) => -accesses.getOrElse(k, 0L)}.toIterator.map{case (k, v) => (k, length(k, v))}.buffered
      val retainKeysWithLength = IteratorUtil.takeMax(sortedKeysWithLength, min - thisLength)(_._2).toList
      val retainKeys = retainKeysWithLength.map(_._1).toSet
      val clearKeys = values.keySet -- retainKeys
      onClear(clearKeys.toSet)
      values.retain{case (k, _) => retainKeys.contains(k)}
      size = retainKeysWithLength.map(_._2).sum
      accesses.clear()
    }

    access(key)
    size += thisLength
    values.update(key, value)
    value
  }

  override def getOrElse(key: K, value: => V): V = {
    access(key)
    values.getOrElse(key, cache(key, value))
  }

  override def remove(key: K): Option[V] = synchronized {
    val option = values.remove(key)
    for (v <- option) size -= length(key, v)
    option
  }
}

object CacheMap {
  def apply[K,V](min: Long, max: Long, onClear: Set[K] => Unit = (_: Set[K]) => {})(length: (K, V) => Long): CacheMap[K,V] = new MinMaxCacheMap(min, max, length, onClear)
  def apply[K,V](max: Long)(length: (K, V) => Long): CacheMap[K,V] = new MinMaxCacheMap(0, max, length, _ => {})
  def empty[K,V]: CacheMap[K,V] = new EmptyCacheMap[K,V]
}
