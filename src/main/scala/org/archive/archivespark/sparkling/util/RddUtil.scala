package org.archive.archivespark.sparkling.util

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.google.common.io.CountingInputStream
import org.apache.commons.io.output.CountingOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner}
import org.archive.archivespark.sparkling.io._
import org.archive.archivespark.sparkling.logging.{Log, LogContext}
import org.archive.archivespark.sparkling.{Sparkling, _}

import scala.reflect.ClassTag

object RddUtil {
  implicit val logContext: LogContext = LogContext(this)

  import Sparkling._

  var saveRecordTimeoutMillis: Int = prop(1000 * 60 * 60)(saveRecordTimeoutMillis, saveRecordTimeoutMillis = _) // 1 hour

  case class RecordsPointer[D: ClassTag](rdd: RDD[D], partitionIdx: Int, offset: Int, length: Int) {
    def get: Array[D] = accessPartitionRange(rdd, partitionIdx, offset, length)
  }

  case class AggregateRecordsPointer[D: ClassTag, A: ClassTag](value: A, records: RecordsPointer[D])

  def shuffle[T: ClassTag](rdd: RDD[T], numPartitions: Int): RDD[T] = initPartitions {
    val partitioner = new HashPartitioner(numPartitions)
    initPartitions(rdd.zipWithIndex.map { case (v, i) => (i, v) }.partitionBy(partitioner).values)
  }

  def parallelize(items: Int): RDD[Int] = parallelize(items, items)

  def parallelize(items: Int, partitions: Int): RDD[Int] = parallelize(0 until items, partitions)

  def parallelize[T: ClassTag](items: Seq[T]): RDD[T] = parallelize(items, items.size)

  def parallelize[T: ClassTag](items: Seq[T], partitions: Int): RDD[T] = initPartitions { shuffle(sc.parallelize(items), partitions.min(items.size)) }

  def collectNoOrder[T: ClassTag](rdd: RDD[T]): Seq[T] = rdd.mapPartitions { records => Iterator((true, records.toList)) }.reduceByKey(_ ++ _, 1).values.take(1).toSeq.flatten

  def reduce[T: ClassTag](rdd: RDD[T])(reduce: (T, T) => T): T = rdd.mapPartitions { records => Iterator((true, records.reduce(reduce))) }.reduceByKey(reduce, 1).values.take(1).head

  def iteratePartitions[D: ClassTag](rdd: RDD[D]): Iterator[Seq[D]] = {
    val persisted = rdd.persist(StorageLevel.MEMORY_AND_DISK)
    persisted.foreachPartition(_ => {})
    val iter = persisted.partitions.indices.toIterator.map { i => sc.runJob(persisted, (iter: Iterator[D]) => iter.toArray, Seq(i)).head }.map(_.toSeq)
    IteratorUtil.cleanup(iter, () => persisted.unpersist())
  }

  def emptyRDD[A: ClassTag]: RDD[A] = sc.parallelize(Seq(false), 1).filter(_ == true).map(_.asInstanceOf[A])

  def loadFilesLocality(path: String, groupFiles: Int = 1, setPartitionFiles: Boolean = true): RDD[String] = initPartitions {
    val rdd = sc.newAPIHadoopFile[NullWritable, Text, FileLocalityInputFormat](path).map(_._2.toString)
    if (setPartitionFiles || groupFiles > 1) {
      val files = HdfsIO.files(path)
      if (files.isEmpty) emptyRDD
      else if (groupFiles > 1) {
        val numFiles = IteratorUtil.count(files)
        rdd.coalesce((numFiles / groupFiles).ceil.toInt)
      } else {
        val filesBc = sc.broadcast(files.toSeq.sorted.zipWithIndex.toMap)
        rdd.mapPartitions { partition =>
          val fileIdx = filesBc.value
          partition.flatMap(f => fileIdx.get(f).map((f, _))).map { case (f, idx) => setTaskInFile(idx, f) }.toArray.toIterator
        }
      }
    } else rdd
  }

  def loadFilesSorted(path: String, groupFiles: Int = 1): RDD[String] = initPartitions {
    val files = HdfsIO.files(path).toSeq.sorted
    if (files.isEmpty) { emptyRDD }
    else {
      if (groupFiles > 1) { RddUtil.parallelize(files.grouped(groupFiles).toSeq).mapPartitions { partition => partition.flatten } }
      else { RddUtil.parallelize(files).mapPartitionsWithIndex { case (idx, partition) => partition.map(setTaskInFile(idx, _)).toArray.toIterator } }
    }
  }

  def loadFileGroups(path: String, group: String => String): RDD[(String, Seq[String])] = {
    val groups = HdfsIO.files(path).toSeq.groupBy(group).mapValues(_.sorted)
    parallelize(groups.toSeq)
  }

  def loadTextFileGroups(path: String, group: String => String, readFully: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy): RDD[(String, Iterator[String])] = {
    loadFileGroups(path, group).map { case (g, files) =>
      (
        g,
        IteratorUtil.lazyFlatMap(files.toIterator) { file =>
          val in = HdfsIO.open(file, length = if (readFully) -1 else 0, strategy = strategy)
          IteratorUtil.cleanup(IOUtil.lines(in), in.close)
        }
      )
    }
  }

  def loadBinary[A: ClassTag](
      path: String,
      decompress: Boolean = true,
      close: Boolean = true,
      readFully: Boolean = false,
      sorted: Boolean = false,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy,
      repartitionFiles: Int = 0,
      groupFiles: Int = 1
  )(action: (String, InputStream) => TraversableOnce[A]): RDD[A] = {
    val files = if (sorted) loadFilesSorted(path, groupFiles = groupFiles) else loadFilesLocality(path, groupFiles = groupFiles)
    val repartitioned = if (repartitionFiles > 0) shuffle(files, repartitionFiles) else files
    lazyFlatMap(repartitioned) { file =>
      if (close) HdfsIO.access(file, decompress = decompress, length = if (readFully) -1 else 0, strategy = strategy) { in => action(file, in) }
      else action(file, HdfsIO.open(file, decompress = decompress, length = if (readFully) -1 else 0, strategy = strategy))
    }
  }

  def loadTyped[A: ClassTag: TypedInOut](
      path: String,
      readFully: Boolean = false,
      sorted: Boolean = false,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy,
      repartitionFiles: Int = 0
  ): RDD[A] = {
    val inout = implicitly[TypedInOut[A]]
    RddUtil.loadBinary(path, decompress = true, close = false, readFully = readFully, sorted = sorted, strategy = strategy, repartitionFiles = repartitionFiles) { (_, in) =>
      IteratorUtil.cleanup(inout.in(in), in.close)
    }
  }

  def loadBinaryLazy[A: ClassTag](
      path: String,
      decompress: Boolean = true,
      close: Boolean = true,
      readFully: Boolean = false,
      sorted: Boolean = false,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy,
      repartitionFiles: Int = 0,
      groupFiles: Int = 1
  )(action: (String, ManagedVal[InputStream]) => TraversableOnce[A]): RDD[A] = {
    val files = if (sorted) loadFilesSorted(path, groupFiles = groupFiles) else loadFilesLocality(path, groupFiles = groupFiles)
    val repartitioned = if (repartitionFiles > 0) shuffle(files, repartitionFiles) else files
    lazyFlatMap(repartitioned) { file =>
      val lazyIn = Common.lazyValWithCleanup(HdfsIO.open(file, decompress = decompress, length = if (readFully) -1 else 0, strategy = strategy))(_.close)
      val r = action(file, lazyIn)
      if (close) lazyIn.clear()
      r
    }
  }

  def loadTextLines(
      path: String,
      readFully: Boolean = false,
      sorted: Boolean = false,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy,
      repartitionFiles: Int = 0,
      groupFiles: Int = 1
  ): RDD[String] = {
    loadBinary(path, close = false, readFully = readFully, sorted = sorted, strategy = strategy, repartitionFiles = repartitionFiles, groupFiles = groupFiles) { (_, in) =>
      IteratorUtil.cleanup(IOUtil.lines(in), in.close)
    }
  }

  def loadTextLinesWithFilenames(
      path: String,
      readFully: Boolean = false,
      sorted: Boolean = false,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy,
      repartitionFiles: Int = 0,
      groupFiles: Int = 1
  ): RDD[(String, String)] = {
    loadBinary(path, close = false, readFully = readFully, sorted = sorted, strategy = strategy, repartitionFiles = repartitionFiles, groupFiles = groupFiles) { (file, in) =>
      IteratorUtil.cleanup(IOUtil.lines(in), in.close).map((file, _))
    }
  }

  def loadTextFiles(
      path: String,
      readFully: Boolean = false,
      sorted: Boolean = false,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy,
      repartitionFiles: Int = 0,
      groupFiles: Int = 1
  ): RDD[(String, CleanupIterator[String])] = {
    loadBinary(path, close = false, readFully = readFully, sorted = sorted, strategy = strategy, repartitionFiles = repartitionFiles, groupFiles = groupFiles) { (file, in) =>
      Some(file, IteratorUtil.cleanup(IOUtil.lines(in), in.close))
    }
  }

  def loadPartitions[A: ClassTag, P: ClassTag: Ordering](path: String, sorted: Boolean = false)(partition: String => Iterator[P])(load: (String, P) => Iterator[A]): RDD[A] = {
    val partitioned = (if (sorted) loadFilesSorted(path) else loadFilesLocality(path)).flatMap { filename => partition(filename).map((_, filename)).toSet }.persist(StorageLevel.MEMORY_AND_DISK)
    val partitionIds = partitioned.map { case (p, f) => p }.distinct().zipWithIndex.collectAsMap
    val partitionIdsBroadcast = sc.broadcast(partitionIds)
    initPartitions(
      partitioned.mapPartitions { records =>
        val partitionIds = partitionIdsBroadcast.value
        records.map { case (p, f) => (partitionIds(p), (p, f)) }
      }.partitionBy(new HashPartitioner(partitionIds.size)).mapPartitions { records => IteratorUtil.lazyFlatMap(records) { case (_, (p, f)) => load(f, p) } }
    )
  }

  def loadTextPartitionsByLines(path: String, linesPerPartition: Int = 100000000, sorted: Boolean = false): RDD[String] = {
    loadTextPartitionsByLinesWithFilenames(path, linesPerPartition, sorted).flatMap(_._2)
  }

  def loadTextPartitionsByLinesWithFilenames(path: String, linesPerPartition: Int = 100000000, sorted: Boolean = false): RDD[(String, CleanupIterator[String])] = {
    loadPartitions(path, sorted = sorted)(f => (0 until (HdfsIO.countLines(f).toDouble / linesPerPartition).ceil.toInt).toIterator.map((_, f))) { case (f, (p, _)) =>
      val in = HdfsIO.open(f)
      val lines = IteratorUtil.drop(IOUtil.lines(in), p * linesPerPartition).take(linesPerPartition)
      Iterator((f, IteratorUtil.cleanup(lines, in.close)))
    }
  }

  def loadTextPartitionsByGroups(path: String, groupBy: String => String, groupsPerPartition: Int = 100000, sorted: Boolean = false): RDD[(String, Iterator[String])] = {
    loadPartitions(path, sorted = sorted)(f => (0 until (IteratorUtil.groupSortedBy(HdfsIO.iterLines(path))(groupBy).size.toDouble / groupsPerPartition).ceil.toInt).toIterator.map((_, f))) {
      case (f, (p, _)) =>
        val in = HdfsIO.open(f)
        val lines = IteratorUtil.drop(IteratorUtil.groupSortedBy(IOUtil.lines(in))(groupBy), p * groupsPerPartition).take(groupsPerPartition)
        IteratorUtil.cleanup(lines, in.close)
    }
  }

  def loadTextPartitionsByBytes(path: String, bytesPerPartition: Long = 1.gb, sorted: Boolean = false): RDD[String] = {
    loadPartitionsByBytes(path, bytesPerPartition, sorted = sorted) { (in, p, last) =>
      if (p > 0) StringUtil.readLine(in) // skip first line
      IteratorUtil.whileDefined { if (last || in.getCount <= bytesPerPartition) Option(StringUtil.readLine(in)) else None }
    }
  }

  def loadPartitionsByBytes[A: ClassTag](path: String, bytesPerPartition: Long = 1.gb, sorted: Boolean = false)(load: (CountingInputStream, Int, Boolean) => Iterator[A]): RDD[A] = {
    loadPartitions(path, sorted = sorted) { f =>
      val fileSize = HdfsIO.length(f)
      val uncompressedSize =
        if (f.toLowerCase.endsWith(GzipExt)) {
          val factor = HdfsIO.access(f, decompress = false)(GzipUtil.estimateCompressionFactor(_, bytesPerPartition))
          (fileSize * factor).ceil.toLong
        } else fileSize
      val numPartitions = (uncompressedSize.toDouble / bytesPerPartition).ceil.toInt
      (0 until numPartitions).toIterator.map(p => ((p, p == numPartitions - 1), f))
    } { case (f, ((p, last), _)) =>
      val in = HdfsIO.open(f)
      IOUtil.skip(in, p * bytesPerPartition)
      val counting = new CountingInputStream(in)
      IteratorUtil.cleanup(load(counting, p, last), counting.close)
    }
  }

  def loadTextLinesGroupedByPrefix(
      path: String,
      prefix: String => String,
      readFully: Boolean = false,
      sorted: Boolean = false,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy
  ): RDD[(String, Iterator[String])] = { groupSorted(loadTextLines(path, readFully = readFully, sorted = sorted, strategy = strategy), prefix) }

  def loadTextLinesGroupedAcrossFiles(path: String, groupBy: String => Option[String], groupFiles: Int = 1): RDD[(String, Iterator[String])] =
    loadGroupedAcrossFiles(path, (file, in) => IOUtil.lines(in, Some(file)), groupFiles)(groupBy)

  def loadGroupedAcrossFiles[D: ClassTag, P](path: String, load: (String, InputStream) => TraversableOnce[D], groupFiles: Int = 1)(groupBy: D => Option[P]): RDD[(P, Iterator[D])] = {
    val files = HdfsIO.files(path).toSeq.sorted.grouped(groupFiles).toSeq
    val filesBroadcast = sc.broadcast(files.map(_.head))

    RddUtil.parallelize(files).flatMap { group =>
      val files = filesBroadcast.value
      val fileIdx = files.indexOf(group.head)
      if (fileIdx < 0) Iterator.empty
      else {
        val fileRecords = CleanupIterator.flatten(group.toIterator.map { file =>
          val in = HdfsIO.open(file, decompress = false)
          IteratorUtil.cleanup(load(file, in).toIterator, in.close)
        })

        val overRecords = CleanupIterator.flatten(files.drop(fileIdx + 1).toIterator.map { file =>
          val in = HdfsIO.open(file, decompress = false)
          IteratorUtil.cleanup(load(file, in).toIterator, in.close)
        }).chain { iter =>
          IteratorUtil.lazyIter {
            val grouped = IteratorUtil.groupSorted(iter, groupBy)
            if (grouped.hasNext) {
              val first = grouped.next
              if (first._1.isDefined) first._2 else Iterator.empty
            } else Iterator.empty
          }
        }

        val records = CleanupIterator.combine(fileRecords, overRecords)
        val grouped = IteratorUtil.groupSorted(records, groupBy)
        if (fileIdx > 0 && grouped.hasNext) grouped.next()

        grouped.filter { case (key, group) => key.isDefined }.map { case (key, group) => (key.get, group) }
      }
    }
  }

  def cogroupStrings(sortedRdd: RDD[String], joinPaths: Seq[String], groupBy: String => String): RDD[(String, Seq[ManagedVal[Iterator[String]]])] = {
    cogroupStrings(sortedRdd, joinPaths, groupBy, identity, None)
  }

  def cogroupStrings(sortedRdd: RDD[String], joinPaths: Seq[String], groupBy: String => String, groupFiles: Int): RDD[(String, Seq[ManagedVal[Iterator[String]]])] = {
    cogroupStrings(sortedRdd, joinPaths, groupBy, identity, None, groupFiles = groupFiles)
  }

  def cogroupStrings(sortedRdd: RDD[String], joinPaths: Seq[String], groupBy: String => String, fromWhile: Option[(String, String => Boolean)]): RDD[(String, Seq[ManagedVal[Iterator[String]]])] = {
    cogroupStrings(sortedRdd, joinPaths, groupBy, identity, fromWhile)
  }

  def cogroupStrings[V: ClassTag](
      sortedRdd: RDD[String],
      joinPaths: Seq[String],
      groupBy: String => String,
      parse: String => V,
      fromWhile: Option[(String, String => Boolean)] = None,
      groupFiles: Int = 1
  ): RDD[(String, Seq[ManagedVal[Iterator[V]]])] = { cogroupWithStrings(groupStrings(sortedRdd, groupBy, parse), joinPaths, groupBy, parse, fromWhile, groupFiles = groupFiles) }

  def cogroupWithStrings(sortedRdd: RDD[(String, ManagedVal[Iterator[String]])], joinPaths: Seq[String], groupBy: String => String): RDD[(String, Seq[ManagedVal[Iterator[String]]])] = {
    cogroupWithStrings(sortedRdd, joinPaths, groupBy, identity, None)
  }

  def cogroupWithStrings(
      sortedRdd: RDD[(String, ManagedVal[Iterator[String]])],
      joinPaths: Seq[String],
      groupBy: String => String,
      fromWhile: Option[(String, String => Boolean)]
  ): RDD[(String, Seq[ManagedVal[Iterator[String]]])] = { cogroupWithStrings(sortedRdd, joinPaths, groupBy, identity, fromWhile) }

  def cogroupWithStrings[V: ClassTag](
      sortedRdd: RDD[(String, ManagedVal[Iterator[V]])],
      joinPaths: Seq[String],
      groupBy: String => String,
      parse: String => V,
      fromWhile: Option[(String, String => Boolean)] = None,
      groupFiles: Int = 1
  ): RDD[(String, Seq[ManagedVal[Iterator[V]]])] = {
    val joinMaps = joinPaths.map(HdfsBackedMap(_, groupBy, parse, cache = false, preloadLength = false, groupFiles = groupFiles))
    val joinMapsBroadcast = sc.broadcast(joinMaps)

    val ends = sortedRdd.mapPartitions { records => Iterator(if (records.hasNext) Some(records.next._1) else None) }.collect
    val endsBroadcast = sc.broadcast(ends)

    sortedRdd.mapPartitionsWithIndex { (idx, records) =>
      val joinMaps = joinMapsBroadcast.value
      val ends = endsBroadcast.value

      val buffered = {
        if (fromWhile.isDefined) {
          val (from, whileCond) = fromWhile.get
          records.dropWhile(_._1 < from).filter(kv => whileCond(kv._1))
        } else records
      }.buffered

      if (buffered.hasNext || idx == 0) {
        val joinStart =
          if (idx == 0) {
            joinMaps.map { map =>
              if (fromWhile.isDefined) {
                val (from, whileCond) = fromWhile.get
                map.from(from).chain(_.takeWhile(kv => whileCond(kv._1)))
              } else map.iter
            }
          } else {
            val (key, _) = buffered.head
            joinMaps.map { map =>
              if (fromWhile.isDefined) {
                val (_, whileCond) = fromWhile.get
                map.from(key).chain(_.takeWhile(kv => whileCond(kv._1)))
              } else map.from(key)
            }
          }

        val end = ends.drop(idx + 1).find(_.isDefined).flatten
        val join = if (end.isDefined) joinStart.map(_.chain(_.takeWhile(_._1 < end.get))) else joinStart

        val cogroups = Seq(buffered) ++ join.map(_.chain(_.map { case (k, v) => (k, v) }))

        val emptyIter = ManagedVal(Seq.empty[V].toIterator)
        IteratorUtil.whileDefined {
          val next = cogroups.filter(_.hasNext)
          if (next.nonEmpty) Some {
            val min = next.map(_.head._1).min
            (min, cogroups.map(iter => if (iter.hasNext && iter.head._1 == min) iter.next()._2 else emptyIter))
          }
          else None
        }
      } else Iterator.empty
    }
  }

  def groupStrings(sortedRdd: RDD[String], groupBy: String => String): RDD[(String, ManagedVal[Iterator[String]])] = { groupStrings(sortedRdd, groupBy, identity) }

  def groupStrings[V: ClassTag](sortedRdd: RDD[String], groupBy: String => String, parse: String => V): RDD[(String, ManagedVal[Iterator[V]])] = {
    sortedRdd.mapPartitions { records => IteratorUtil.groupSortedBy(records)(groupBy).map { case (group, iter) => group -> ManagedVal(iter.map(parse)) } }
  }

  def filterKeyStrings(sortedRdd: RDD[String], keysPath: String, key: String => String, mapKeys: String => String = identity): RDD[(String, Boolean)] = {
    cogroupWithStrings(groupStrings(sortedRdd, key, identity), Seq(keysPath), mapKeys).flatMap { case (_, records) =>
      val available = records(1).option.exists(_.hasNext)
      records.head.option.toIterator.flatMap { iter => iter.map((_, available)) }
    }
  }

  def loadCogroupedStrings(path: String, joinPaths: Seq[String], groupBy: String => String): RDD[(String, Seq[ManagedVal[Iterator[String]]])] = {
    loadCogroupedStrings(path, joinPaths, groupBy, identity)
  }

  def loadCogroupedStrings[V: ClassTag](path: String, joinPaths: Seq[String], groupBy: String => String, parse: String => V): RDD[(String, Seq[ManagedVal[Iterator[V]]])] = {
    val sortedRdd = loadTextLinesGroupedAcrossFiles(path, groupBy = str => Some(groupBy(str))).map { case (group, records) => group -> ManagedVal(records.map(parse)) }
    cogroupWithStrings(sortedRdd, joinPaths, groupBy, parse)
  }

  def doPartitions[A: ClassTag](rdd: RDD[A])(action: Int => Unit): RDD[A] = rdd.mapPartitionsWithIndex(
    (idx, records) => {
      action(idx)
      records
    },
    preservesPartitioning = true
  )

  def groupSorted[A, B](rdd: RDD[A], groupBy: A => B): RDD[(B, Iterator[A])] = rdd.mapPartitions(IteratorUtil.groupSorted(_, groupBy))

  def distinct[D: ClassTag](rdd: RDD[D], subtract: TraversableOnce[RDD[D]] = Seq.empty, partitions: Int = parallelism): RDD[D] = distinct(rdd, subtract, new HashPartitioner(partitions))

  def distinct[D: ClassTag](rdd: RDD[D], partitioner: Partitioner): RDD[D] = distinct(rdd, Seq.empty, partitioner)

  def distinct[D: ClassTag](rdd: RDD[D], subtract: TraversableOnce[RDD[D]], partitioner: Partitioner): RDD[D] = {
    var distinct = rdd.mapPartitions { records =>
      val distinct = records.toSet
      distinct.toIterator.map(s => (s, true))
    }.partitionBy(partitioner)

    for (s <- subtract) {
      distinct = distinct.subtract(
        s.mapPartitions { records =>
          val distinct = records.toSet
          distinct.toIterator.map(s => (s, true))
        }.partitionBy(partitioner),
        partitioner
      )
    }

    distinct.mapPartitions(_.map { case (record, _) => record }.toSet.toIterator)
  }

  def distinctSorted[A: ClassTag](rdd: RDD[A]): RDD[A] = lazyMapPartitions(rdd) { (idx, records) => IteratorUtil.distinctOrdered(records) }

  def iterateDistinctPartitions[D: ClassTag](rdd: RDD[D], subtract: TraversableOnce[RDD[D]] = Seq.empty, partitions: Int = parallelism): Iterator[Set[D]] = {
    val partitioner = new HashPartitioner(partitions)
    val repartitioned = shuffle(distinct(rdd, subtract, partitioner), partitions)
    iteratePartitions(repartitioned).map(_.toSet)
  }

  def iterate[D: ClassTag](rdd: RDD[D], bufferSize: Int = 1000): CleanupIterator[D] = {
    val persisted = rdd.persist(StorageLevel.MEMORY_AND_DISK)
    persisted.foreachPartition(_ => {})
    val iter = persisted.partitions.indices.toIterator.flatMap { i =>
      var start = 0
      var hasNext = true
      Iterator.continually {
        if (hasNext) {
          val buffer = sc.runJob(persisted, (iter: Iterator[D]) => iter.slice(start, start + bufferSize + 1).toArray, Seq(i)).head
          start += bufferSize
          hasNext = buffer.length > bufferSize
          buffer.take(bufferSize)
        } else Array.empty
      }.takeWhile(_.nonEmpty).flatMap(array => array)
    }
    IteratorUtil.cleanup(iter, () => persisted.unpersist())
  }

  def iterateAggregates[D: ClassTag, A: ClassTag](rdd: RDD[D], bufferSize: Int = 1000)(aggregate: Seq[D] => A): CleanupIterator[AggregateRecordsPointer[D, A]] = {
    iterate(
      rdd.mapPartitionsWithIndex { (idx, records) => records.grouped(bufferSize).zipWithIndex.map { case (group, i) => (aggregate(group), idx, i * bufferSize, bufferSize) } },
      bufferSize
    ).chain(_.map { case (v, p, o, l) => AggregateRecordsPointer(v, RecordsPointer(rdd, p, o, l)) })
  }

  def accessPartitionRange[D: ClassTag](rdd: RDD[D], pointer: RecordsPointer[D]): Array[D] = accessPartitionRange(rdd, pointer.partitionIdx, pointer.offset, pointer.length)

  def accessPartitionRange[D: ClassTag](rdd: RDD[D], partitionIdx: Int, offset: Int, length: Int): Array[D] = {
    sc.runJob(rdd, (iter: Iterator[D]) => iter.slice(offset, offset + length).toArray, Seq(partitionIdx)).head
  }

  def saveAsNamedTextFile(
      rdd: => RDD[(String, String)],
      path: String,
      partitions: Int = Sparkling.parallelism,
      repartition: Boolean = false,
      sorted: Boolean = false,
      skipIfExists: Boolean = false
  ): Long = {
    val completeFlagFile = path + "/" + CompleteFlagFile
    if (skipIfExists && HdfsIO.exists(completeFlagFile)) return 0L
    HdfsIO.ensureOutDir(path)
    val repartitioned = if (repartition) rdd.partitionBy(new HashPartitioner(partitions)) else rdd
    val processed = repartitioned.mapPartitionsWithIndex { case (idx, records) =>
      if (records.hasNext) {
        Common.timeoutWithReporter(saveRecordTimeoutMillis) { reporter =>
          val streams = collection.mutable.Map.empty[String, PrintStream]
          var prevFile: String = null
          var currentStream: PrintStream = null
          var processed: Long = 0L
          for ((file, value) <- records) {
            reporter.alive("Writing [" + value + "]")
            val filename =
              if (repartition || sorted) file
              else {
                val split = file.split("\\.", 2)
                val (filePrefix, fileExt) = (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
                filePrefix + "-" + StringUtil.padNum(idx, 5) + fileExt
              }
            val outPath = new Path(path, filename).toString
            val out =
              if (sorted) {
                if (prevFile == filename) currentStream
                else {
                  processed += 1
                  prevFile = filename
                  if (currentStream != null) currentStream.close()
                  currentStream = IOUtil.print(HdfsIO.out(outPath, overwrite = true))
                  currentStream
                }
              } else streams.getOrElseUpdate(filename, IOUtil.print(HdfsIO.out(outPath, overwrite = true)))
            out.println(value)
          }
          streams.values.foreach(_.close())
          if (currentStream != null) currentStream.close()
          Iterator(if (processed > 0) processed else streams.size)
        }
      } else Iterator(0L)
    }.reduce(_ + _)
    HdfsIO.touch(completeFlagFile)
    processed
  }

  def savePartitions[A](rdd: => RDD[A], path: String, compress: Boolean = true, skipIfExists: Boolean = false, checkPerFile: Boolean = false, skipEmpty: Boolean = true)(
      action: (Iterator[A], OutputStream, Common.ProcessReporter) => Long
  ): Long = {
    val completeFlagFile = path + "/" + CompleteFlagFile
    if (skipIfExists && HdfsIO.exists(completeFlagFile)) return 0L
    HdfsIO.ensureOutDir(path, ensureNew = !(skipIfExists && checkPerFile))
    val fileName = new Path(path).getName
    val split = fileName.split("\\.", 2)
    val (filePrefix, fileExt) = (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
    val processed =
      if (rdd.getNumPartitions == 0) 0L
      else rdd.mapPartitions { records =>
        val outPath = new Path(path, Sparkling.getTaskOutFile(idx => filePrefix + "-" + StringUtil.padNum(idx, 5) + fileExt)).toString
        if (skipIfExists && HdfsIO.exists(outPath)) Iterator(0L)
        else {
          if (skipEmpty && !records.hasNext) Iterator(0L)
          else {
            val out = HdfsIO.out(outPath, compress = compress, overwrite = true)
            val processed = if (records.hasNext) Common.timeoutWithReporter(saveRecordTimeoutMillis)(action(records, out, _)) else 0L
            out.close()
            Iterator(processed)
          }
        }
      }.reduce(_ + _)
    HdfsIO.touch(completeFlagFile)
    processed
  }

  def saveTyped[A: TypedInOut](rdd: => RDD[A], path: String, skipIfExists: Boolean = false, checkPerFile: Boolean = false, skipEmpty: Boolean = true): Long = {
    val inout = implicitly[TypedInOut[A]]
    savePartitions(rdd, path, skipIfExists = skipIfExists, checkPerFile = checkPerFile, skipEmpty = skipEmpty) { (records, out, reporter) =>
      val writer = inout.out(new NonClosingOutputStream(out))
      val count = records.map { r =>
        reporter.alive()
        writer.write(r)
        1L
      }.sum
      writer.close()
      count
    }
  }

  def saveAsTextFile(rdd: => RDD[String], path: String, skipIfExists: Boolean = false, checkPerFile: Boolean = false, skipEmpty: Boolean = true): Long = {
    savePartitions(rdd, path, skipIfExists = skipIfExists, checkPerFile = checkPerFile, skipEmpty = skipEmpty) { (records, out, reporter) =>
      val print = IOUtil.print(out, closing = false)
      val count = records.map { r =>
        reporter.alive("Writing [" + r + "]")
        print.println(r)
        reporter.alive("Done writing [" + r + "]")
        1L
      }.sum
      print.close()
      count
    }
  }

  def saveTextWithIndex(
      rdd: => RDD[String],
      path: String,
      compress: Boolean = true,
      skipIfExists: Boolean = false,
      checkPerFile: Boolean = false,
      skipEmpty: Boolean = true,
      maxLines: Int = -1,
      key: String => String = identity,
      groupByKey: Boolean = false
  ): Long = {
    val completeFlagFile = path + "/" + CompleteFlagFile
    if (skipIfExists && HdfsIO.exists(completeFlagFile)) return 0L
    HdfsIO.ensureOutDir(path, ensureNew = !(skipIfExists && checkPerFile))
    val fileName = new Path(path).getName
    val split = fileName.split("\\.", 2)
    val (filePrefix, fileExt) = (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
    val processed = rdd.mapPartitions { records =>
      val outPath = new Path(path, Sparkling.getTaskOutFile(idx => filePrefix + "-" + StringUtil.padNum(idx, 5) + fileExt)).toString
      val compressed = outPath.toLowerCase.endsWith(GzipExt)
      val idxPath = outPath + IdxExt
      if (skipIfExists && HdfsIO.exists(outPath)) Iterator(0L)
      else {
        if (skipEmpty && !records.hasNext) Iterator(0L)
        else {
          val processed =
            if (records.hasNext) {
              val idxOut = IOUtil.print(HdfsIO.out(idxPath, compress = false, overwrite = true), closing = true)
              val grouped =
                if (groupByKey) {
                  if (maxLines > 0) { IteratorUtil.groupSorted(records, key).flatMap { case (p, r) => IteratorUtil.groupedN(r, maxLines).map((p, _)) } }
                  else IteratorUtil.groupSorted(records, key)
                } else {
                  (if (maxLines > 0) IteratorUtil.groupedN(records, maxLines) else Iterator(records)).flatMap { group =>
                    val head = group.next
                    Iterator((key(head), Iterator(head) ++ group))
                  }
                }
              val tmpOutPath = IOUtil.tmpFile.toString
              val tmpOut = new CountingOutputStream(IOUtil.fileOut(tmpOutPath))
              var offset = 0L
              val processed = Common.timeoutWithReporter(saveRecordTimeoutMillis) { reporter =>
                grouped.map { case (p, group) =>
                  val out = if (compressed) new GZIPOutputStream(new NonClosingOutputStream(tmpOut)) else new NonClosingOutputStream(tmpOut)
                  val print = IOUtil.print(out, closing = true)
                  val count = group.map { r =>
                    reporter.alive("Writing [" + r + "]")
                    print.println(r)
                    reporter.alive("Done writing [" + r + "]")
                    1L
                  }.sum
                  print.close()
                  val length = tmpOut.getByteCount
                  idxOut.println(p + "\t" + count + "\t" + offset + "\t" + (length - offset))
                  offset = length
                  count
                }.sum
              }
              tmpOut.close()
              HdfsIO.copyFromLocal(tmpOutPath, outPath, move = true, overwrite = true)
              idxOut.close()
              processed
            } else 0L
          Iterator(processed)
        }
      }
    }.reduce(_ + _)
    HdfsIO.touch(completeFlagFile)
    processed
  }

  def saveSplits[A](rdd: => RDD[Iterator[A]], path: String, skipIfExists: Boolean = false, checkPerFile: Boolean = false)(action: (Iterator[A], OutputStream, Common.ProcessReporter) => Long): Long = {
    val completeFlagFile = path + "/" + CompleteFlagFile
    if (skipIfExists && HdfsIO.exists(completeFlagFile)) return 0L
    HdfsIO.ensureOutDir(path, ensureNew = !(skipIfExists && checkPerFile))
    val pathFileName = new Path(path).getName
    val split = pathFileName.split("\\.", 2)
    val (pathFilePrefix, pathFileExt) = (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
    val processed = rdd.mapPartitionsWithIndex { (idx, splits) =>
      val (filePrefix, fileExt) = {
        val split = RegexUtil.split(Sparkling.getTaskOutFile(idx => pathFilePrefix + "-" + StringUtil.padNum(idx, 5) + pathFileExt), "\\.", 2)
        (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
      }
      val completeSplitFlagFile = path + "/_" + filePrefix + fileExt.replace('.', '_') + CompleteFlagFile
      if (skipIfExists && HdfsIO.exists(completeSplitFlagFile)) Iterator(0L)
      else {
        val processed = splits.zipWithIndex.map { case (records, i) =>
          val outPath = new Path(path, filePrefix + "-" + StringUtil.padNum(i, 5) + fileExt).toString
          val out = HdfsIO.out(outPath, overwrite = true)
          val processed = if (records.hasNext) Common.timeoutWithReporter(saveRecordTimeoutMillis)(action(records, out, _)) else 0L
          out.close()
          processed
        }.sum
        HdfsIO.touch(completeSplitFlagFile)
        Iterator(processed)
      }
    }.reduce(_ + _)
    HdfsIO.touch(completeFlagFile)
    processed
  }

  def saveTextSplits(rdd: => RDD[String], path: String, max: Long, length: String => Long, skipIfExists: Boolean = true, checkPerFile: Boolean = false): Long = {
    saveSplits(rdd.mapPartitions(records => IteratorUtil.grouped(records, max)(length)), path, skipIfExists, checkPerFile) { (records, out, reporter) =>
      val print = IOUtil.print(out, closing = false)
      val count = records.map { r =>
        reporter.alive("Writing [" + r + "]")
        print.println(r)
        1L
      }.sum
      print.close()
      count
    }
  }

  def saveTextSplitsGrouped(rdd: => RDD[String], path: String, max: Long, length: String => Long, skipIfExists: Boolean = true, checkPerFile: Boolean = false)(groupBy: String => String): Long = {
    saveSplits(
      rdd.mapPartitions { records => IteratorUtil.groupedGroups(IteratorUtil.groupSorted(records, groupBy).map(_._2), max)(length) },
      path,
      skipIfExists,
      checkPerFile
    ) { (records, out, reporter) =>
      val print = IOUtil.print(out, closing = false)
      val count = records.map { r =>
        reporter.alive("Writing [" + r + "]")
        print.println(r)
        1L
      }.sum
      print.close()
      count
    }
  }

  def saveTextSplitsByLines(rdd: => RDD[String], path: String, lines: Int = 1000000, skipIfExists: Boolean = true, checkPerFile: Boolean = false): Long =
    saveTextSplits(rdd, path, lines, _ => 1L, skipIfExists, checkPerFile)

  def saveTextSplitsByBytes(rdd: => RDD[String], path: String, bytes: Long = 1.gb, skipIfExists: Boolean = true, checkPerFile: Boolean = false): Long =
    saveTextSplits(rdd, path, bytes, _.length, skipIfExists, checkPerFile)

  def collectDistinct[A: ClassTag](rdd: RDD[A], minus: Set[A] = Set.empty[A]): Set[A] = {
    val bc = sc.broadcast(minus)
    rdd.mapPartitions(records => Iterator(records.toSet -- bc.value)).reduce(_ ++ _)
  }

  def sortByAndWithinPartitions[A: ClassTag, P: ClassTag: Ordering, S: ClassTag](rdd: RDD[A], partitions: Int = parallelism, ascending: Boolean = true)(
      by: A => (P, S)
  )(implicit ordering: Ordering[(P, S)]): RDD[A] = {
    val withKey = rdd.keyBy(by)
    val partitioner = new PartialKeyRangePartitioner[(P, S), A, P](partitions, withKey, _._1, ascending)
    new ShuffledRDD[(P, S), A, A](withKey, partitioner).setKeyOrdering(if (ascending) ordering else ordering.reverse).values
  }

  def repartitionByAndSort[A: ClassTag, S: ClassTag: Ordering, P: ClassTag](rdd: RDD[(S, A)], partitions: Int = parallelism, ascending: Boolean = true)(
      by: S => P
  )(implicit ordering: Ordering[S]): RDD[(S, A)] = {
    val partitioner = new PartialKeyPartitioner[S, P](partitions, by)
    new ShuffledRDD[S, A, A](rdd, partitioner).setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  def repartitionByPrimaryAndSort[A: ClassTag, P: ClassTag, S: ClassTag](rdd: RDD[((P, S), A)], partitions: Int = parallelism, ascending: Boolean = true)(implicit
      ordering: Ordering[(P, S)]
  ): RDD[((P, S), A)] = {
    val partitioner = new PrimaryKeyPartitioner(partitions)
    new ShuffledRDD[(P, S), A, A](rdd, partitioner).setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  def lazyFlatMap[A: ClassTag, B: ClassTag](rdd: RDD[A])(map: A => TraversableOnce[B]): RDD[B] = lazyMapPartitions(rdd) { (_, records) => records.flatMap(map) }

  def lazyMapPartitions[A: ClassTag, B: ClassTag](rdd: RDD[A])(map: (Int, Iterator[A]) => Iterator[B]): RDD[B] = initPartitions(rdd).mapPartitionsWithIndex { (idx, records) =>
    IteratorUtil.lazyIter(map(idx, records))
  }

  def cache[A: ClassTag: TypedInOut](rdd: RDD[A]): RDD[A] = initPartitions {
    val inout = implicitly[TypedInOut[A]]
    val cacheId = System.nanoTime.toString
    val cached = lazyMapPartitions(rdd) { (idx, records) =>
      Common.timeoutWithReporter(saveRecordTimeoutMillis) { reporter =>
        Log.info("Caching records...")
        val dir = new File(Sparkling.LocalCacheDir).getCanonicalFile
        dir.mkdirs()
        val filename = cacheId + "-partition-" + StringUtil.padNum(idx, 5) + TmpExt + GzipExt
        val cacheFile = new File(dir, filename)
        val out = inout.out(new GZIPOutputStream(IOUtil.fileOut(cacheFile)))
        var count = 0
        for (r <- records) {
          reporter.alive("Caching record " + count)
          out.write(r)
          count += 1
        }
        Log.info("Caching done.")
        out.close()
        Iterator(cacheFile.getCanonicalPath)
      }
    }.persist(StorageLevel.DISK_ONLY)
    lazyMapPartitions(new CacheLayerRDD(cached)) { (idx, records) =>
      val in = new GZIPInputStream(new FileInputStream(records.next))
      IteratorUtil.cleanup(inout.in(in), in.close)
    }
  }

  def logProgress[A: ClassTag](rdd: RDD[A], id: Option[String] = None, logMod: Int = 1, chunkSize: Int = -1, state: Option[A => String] = None): RDD[A] = {
    lazyMapPartitions(rdd) { (idx, records) =>
      var count = 0
      var chunkNo = 0
      if (chunkSize > 0) {
        records.grouped(chunkSize).flatMap { chunk =>
          chunkNo += 1
          val chunkTotal = chunk.size
          var chunkCount = 0
          chunk.map { r =>
            count += 1
            chunkCount += 1
            if (count % logMod == 0) Log.info(
              s"Progress log${id.map(" [" + _ + "]").getOrElse("")}: Processed ${StringUtil.formatNumber(count)} [${StringUtil.formatNumber(chunkCount)} / ${StringUtil.formatNumber(chunkTotal)} in chunk $chunkNo]..." +
                state.map(_(r)).map(" (" + _ + ")").getOrElse("")
            )
            r
          }
        }
      } else {
        records.map { r =>
          count += 1
          if (count % logMod == 0) Log.info(s"Progress log${id.map(" [" + _ + "]").getOrElse("")}: Processed ${StringUtil.formatNumber(count)}..." + state.map(_(r)).map(" (" + _ + ")").getOrElse(""))
          r
        }
      }
    }
  }
}
