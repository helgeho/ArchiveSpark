package org.archive.archivespark.sparkling.io

import org.archive.archivespark.sparkling.io.HdfsBackedMapOperations.{HdfsBackedSubtractKeysMap, HdfsBackedSubtractMap}
import org.archive.archivespark.sparkling.util.{CleanupIterator, IteratorUtil, RddUtil}
import org.archive.archivespark.sparkling._

trait PrimitiveHdfsBackedMap extends Serializable {
  def key: String => String
  def get(key: String): Option[CleanupIterator[String]]
  def cache: Boolean
  def preloadLength: Boolean
  def iter: CleanupIterator[(String, Iterator[String])]
  def from(key: String, lookupPrefixes: Boolean = false): CleanupIterator[(String, Iterator[String])]
  def before(key: String): CleanupIterator[(String, Iterator[String])]
  def fromWithIndex(key: String, lookupPrefixes: Boolean = false): CleanupIterator[((String, Iterator[String]), Int)]
  def dropKeys(n: Int): CleanupIterator[(String, Iterator[String])]

  def subtractKeys(paths: Set[String]): PrimitiveHdfsBackedMap = new HdfsBackedSubtractKeysMap(this, paths.toSeq.map(new PrimitiveHdfsBackedMapImpl(_, key, cache, preloadLength)))
  def subtract(paths: Set[String]): PrimitiveHdfsBackedMap = new HdfsBackedSubtractMap(this, paths.toSeq.map(new PrimitiveHdfsBackedMapImpl(_, key, cache, preloadLength)))
}

class PrimitiveHdfsBackedMapImpl private[io] (val path: String, val key: String => String, val cache: Boolean, val preloadLength: Boolean, groupFiles: Int = 1) extends PrimitiveHdfsBackedMap {
  val CacheBufferSize: Int = 1.kb.toInt

  private val files: Seq[(Seq[String], String, String, Int)] = {
    RddUtil.loadTextFiles(
      path,
      readFully = preloadLength,
      strategy = if (preloadLength) HdfsIO.LoadingStrategy.CopyLocal else HdfsIO.LoadingStrategy.Remote,
      groupFiles = groupFiles,
      sorted = groupFiles > 1
    ).mapPartitions { fileGroup =>
      if (fileGroup.hasNext) {
        if (preloadLength) {
          fileGroup.flatMap { case (filename, file) =>
            file.chain(_.filter(_.trim.nonEmpty)).iter { lines =>
              val first = lines.headOption.map(key)
              var last = first
              var count = 1
              for (line <- lines) {
                val k = key(line)
                if (!last.contains(k)) {
                  count += 1
                  last = Some(k)
                }
              }
              first.map { f => (Seq(filename), f, last.get, count) }
            }
          }
        } else {
          if (cache) {
            fileGroup.flatMap { case (filename, file) => file.chain(_.filter(_.trim.nonEmpty)).iter { lines => lines.headOption.toIterator.map(key).map { f => (Seq(filename), f, f, 0) } } }
          } else {
            val (filename, file) = fileGroup.next
            file.chain(_.filter(_.trim.nonEmpty)).iter { lines => lines.headOption.toIterator.map(key).map { f => (Seq(filename) ++ fileGroup.map(_._1).toList, f, f, 0) } }
          }
        }
      } else Iterator.empty
    }.collect.sortBy { case (files, first, last, lines) => (first, files.head) }
  }

  private def iterFiles(files: Seq[String]): CleanupIterator[(String, Iterator[String])] = {
    var prevCache: Option[CleanupIterator[String]] = None
    CleanupIterator.flatten(files.toIterator.map { file =>
      val in = HdfsIO.open(file)
      IteratorUtil.cleanup(IOUtil.lines(in).filter(_.trim.nonEmpty).map(l => (file, key(l), l)), in.close)
    }).onClear { () => for (c <- prevCache) c.clear(false) }.chain { lines =>
      val grouped = IteratorUtil.groupSortedBy(lines)(_._2)
      if (cache) {
        grouped.map { case (k, v) =>
          for (c <- prevCache) c.clear(false)
          val buffered = v.buffered
          val file = buffered.head._1
          val values = IOUtil.buffer(bufferSize = CacheBufferSize, lazyEval = false) { out => IOUtil.writeLines(out, buffered.map(_._3)) }
          prevCache = Some(HdfsBackedMap.cache(k, file, values))
          (k, prevCache.get)
        }
      } else { grouped.map { case (k, v) => (k, v.map(_._3)) } }
    }
  }

  def get(key: String): Option[CleanupIterator[String]] = {
    if (preloadLength) files.find { case (_, first, last, _) => first <= key && last >= key }
    else IteratorUtil.zipNext(files.toIterator).find { case ((_, first, _, _), next) => first <= key && (next.isEmpty || next.get._2 > key) }.map(_._1)
  }.map(_._1).flatMap { files =>
    (if (cache) HdfsBackedMap.cached(key, files.head) else None).orElse { iterFiles(files).chainOpt { iter => iter.dropWhile(_._1 < key).toSeq.headOption.filter(_._1 == key).map(_._2) } }
  }

  def iter: CleanupIterator[(String, Iterator[String])] = iterFiles(files.flatMap(_._1))

  def from(key: String, lookupPrefixes: Boolean = false): CleanupIterator[(String, Iterator[String])] = {
    val relevantFiles =
      if (preloadLength) files.dropWhile { case (_, _, last, _) => last < key && (!lookupPrefixes || !key.startsWith(last)) }
      else IteratorUtil.dropButLast(files.toIterator.buffered) { case (_, first, _, _) => first < key && (!lookupPrefixes || !key.startsWith(first)) }.toSeq
    iterFiles(relevantFiles.flatMap(_._1)).chain { iter => iter.dropWhile { case (k, _) => k < key && (!lookupPrefixes || !key.startsWith(k)) } }
  }

  def before(key: String): CleanupIterator[(String, Iterator[String])] = {
    val relevantFiles = IteratorUtil.dropButLast(files.toIterator.buffered) { case (_, first, _, _) => first < key }.toSeq
    iterFiles(relevantFiles.flatMap(_._1)).chain { iter => IteratorUtil.dropButLast(iter) { case (k, _) => k < key } }
  }

  def fromWithIndex(key: String, lookupPrefixes: Boolean = false): CleanupIterator[((String, Iterator[String]), Int)] = {
    var currentIndex = 0
    val relevantFiles =
      if (preloadLength) files.dropWhile { case (_, _, last, lines) =>
        if (last < key && (!lookupPrefixes || !key.startsWith(last))) {
          currentIndex += lines
          true
        } else false
      }
      else files
    iterFiles(relevantFiles.flatMap(_._1)).chain(_.zipWithIndex.map { case (r, idx) => (r, idx + currentIndex) }.dropWhile { case ((k, _), _) => k < key && (!lookupPrefixes || !key.startsWith(k)) })
  }

  def dropKeys(n: Int): CleanupIterator[(String, Iterator[String])] = {
    var currentIndex = 0
    val relevantFiles =
      if (preloadLength) files.dropWhile { case (_, _, _, lines) =>
        if (currentIndex + lines < n + 1) {
          currentIndex += lines
          true
        } else false
      }
      else files
    iterFiles(relevantFiles.flatMap(_._1)).chain(_.drop(n - currentIndex))
  }
}
