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

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.google.common.io.{CountingInputStream, Files}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner}
import org.archive.archivespark.sparkling._
import org.archive.archivespark.sparkling.io._

import scala.reflect.ClassTag

object RddUtil {
  import Sparkling._

  case class RecordsPointer[D : ClassTag](rdd: RDD[D], partitionIdx: Int, offset: Int, length: Int) {
    def get: Array[D] = accessPartitionRange(rdd, partitionIdx, offset, length)
  }

  case class AggregateRecordsPointer[D : ClassTag, A : ClassTag](value: A, records: RecordsPointer[D])

  def shuffle[T : ClassTag](rdd: RDD[T], numPartitions: Int): RDD[T] = {
    val partitioner = new HashPartitioner(numPartitions)
    rdd.zipWithIndex.map{case (v, i) => (i, v)}.partitionBy(partitioner).values
  }

  def parallelize(items: Int): RDD[Int] = parallelize(items, items)

  def parallelize(items: Int, partitions: Int): RDD[Int] = parallelize(0 until items, partitions)

  def parallelize[T : ClassTag](items: Seq[T]): RDD[T] = parallelize(items, items.size)

  def parallelize[T : ClassTag](items: Seq[T], partitions: Int): RDD[T] = initPartitions {
    shuffle(sc.parallelize(items), partitions.min(items.size))
  }

  def iteratePartitions[D : ClassTag](rdd: RDD[D]): Iterator[Seq[D]] = {
    val persisted = rdd.persist(StorageLevel.MEMORY_AND_DISK)
    persisted.foreachPartition(_ => {})
    val iter = persisted.partitions.indices.toIterator.map { i =>
      sc.runJob(persisted, (iter: Iterator[D]) => iter.toArray, Seq(i)).head
    }.map(_.toSeq)
    IteratorUtil.cleanup(iter, () => persisted.unpersist())
  }

  def loadFilesLocality(path: String): RDD[String] = initPartitions {
    sc.newAPIHadoopFile[NullWritable, Text, FileLocalityInputFormat](path).mapPartitions(_.map{case (_, file) => setTaskInFile(file.toString)}.toArray.toIterator)
  }

  def loadFilesSorted(path: String): RDD[String] = initPartitions {
    RddUtil.parallelize(HdfsIO.files(path).toSeq).mapPartitions(_.map(setTaskInFile).toArray.toIterator)
  }

  def loadFileGroups(path: String, group: String => String): RDD[(String, Seq[String])] = {
    val groups = HdfsIO.files(path).toSeq.groupBy(group).mapValues(_.sorted)
    parallelize(groups.toSeq)
  }

  def loadTextFileGroups(path: String, group: String => String, readFully: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy): RDD[(String, Iterator[String])] = {
    loadFileGroups(path, group).map { case (g, files) =>
      (g, IteratorUtil.lazyFlatMap(files.toIterator) { file =>
        val in = HdfsIO.open(file, length = if (readFully) -1 else 0, strategy = strategy)
        IteratorUtil.cleanup(IOUtil.lines(in), in.close)
      })
    }
  }

  def loadBinary[A : ClassTag](path: String, decompress: Boolean = true, close: Boolean = true, readFully: Boolean = false, sorted: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy, repartitionFiles: Int = 0)(action: (String, InputStream) => TraversableOnce[A]): RDD[A] = {
    val files = if (sorted) loadFilesSorted(path) else loadFilesLocality(path)
    val repartitioned = if (repartitionFiles > 0) shuffle(files, repartitionFiles) else files
    lazyFlatMap(repartitioned) { file =>
      if (close) HdfsIO.access(file, decompress = decompress, length = if (readFully) -1 else 0, strategy = strategy) { in => action(file, in) }
      else action(file, HdfsIO.open(file, decompress = decompress, length = if (readFully) -1 else 0, strategy = strategy))
    }
  }

  def loadTyped[A : ClassTag : TypedInOut](path: String, readFully: Boolean = false, sorted: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy, repartitionFiles: Int = 0): RDD[A] = {
    val inout = implicitly[TypedInOut[A]]
    RddUtil.loadBinary(path, decompress = true, close = false, readFully = readFully, sorted = sorted, strategy = strategy, repartitionFiles = repartitionFiles) { (_, in) => IteratorUtil.cleanup(inout.in(in), in.close) }
  }

  def loadBinaryLazy[A : ClassTag](path: String, decompress: Boolean = true, close: Boolean = true, readFully: Boolean = false, sorted: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy, repartitionFiles: Int = 0)(action: (String, ManagedVal[InputStream]) => TraversableOnce[A]): RDD[A] = {
    val files = if (sorted) loadFilesSorted(path) else loadFilesLocality(path)
    val repartitioned = if (repartitionFiles > 0) shuffle(files, repartitionFiles) else files
    lazyFlatMap(repartitioned) { file =>
      val lazyIn = Common.lazyValWithCleanup(HdfsIO.open(file, decompress = decompress, length = if (readFully) -1 else 0, strategy = strategy))(_.close)
      val r = action(file, lazyIn)
      if (close) lazyIn.clear()
      r
    }
  }

  def loadTextLines(path: String, readFully: Boolean = false, sorted: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy, repartitionFiles: Int = 0): RDD[String] = {
    loadBinary(path, close = false, readFully = readFully, sorted = sorted, strategy = strategy, repartitionFiles = repartitionFiles) { (_, in) =>
      IteratorUtil.cleanup(IOUtil.lines(in), in.close)
    }
  }

  def loadTextLinesWithFilenames(path: String, readFully: Boolean = false, sorted: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy, repartitionFiles: Int = 0): RDD[(String, String)] = {
    loadBinary(path, close = false, readFully = readFully, sorted = sorted, strategy = strategy, repartitionFiles = repartitionFiles) { (file, in) =>
      IteratorUtil.cleanup(IOUtil.lines(in), in.close).map((file, _))
    }
  }

  def loadTextFiles(path: String, readFully: Boolean = false, sorted: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy, repartitionFiles: Int = 0): RDD[(String, Iterator[String])] = {
    loadBinary(path, close = false, readFully = readFully, sorted = sorted, strategy = strategy, repartitionFiles = repartitionFiles) { (file, in) =>
      Some(file, IteratorUtil.cleanup(IOUtil.lines(in), in.close))
    }
  }

  def loadPartitions[A : ClassTag, P : ClassTag : Ordering](path: String)(partition: String => Iterator[P])(load: (String, P) => Iterator[A]): RDD[A] = {
    val partitioned = loadFilesLocality(path).flatMap{filename => partition(filename).map((_, filename)).toSet}.persist(StorageLevel.MEMORY_AND_DISK)
    val partitionIds = partitioned.map{case (p, f) => p}.distinct().zipWithIndex().collectAsMap()
    val partitionIdsBroadcast = sc.broadcast(partitionIds)
    partitioned.mapPartitions { records =>
      val partitionIds = partitionIdsBroadcast.value
      records.map{case (p,f) => (partitionIds(p), (p,f))}
    }.partitionBy(new HashPartitioner(partitionIds.size)).mapPartitions { records =>
      IteratorUtil.lazyFlatMap(records) { case (_, (p, f)) =>
        load(f, p)
      }
    }
  }

  def loadTextPartitionsByLines(path: String, linesPerPartition: Int = 100000000): RDD[String] = {
    loadPartitions(path)(f => (0 until (HdfsIO.countLines(f).toDouble / linesPerPartition).ceil.toInt).toIterator.map((_, f))) { case (f, (p, _)) =>
      val in = HdfsIO.open(f)
      val lines = IteratorUtil.drop(IOUtil.lines(in), p * linesPerPartition).take(linesPerPartition)
      IteratorUtil.cleanup(lines, in.close)
    }
  }

  def loadTextPartitionsByBytes(path: String, bytesPerPartition: Long = 10.gb): RDD[String] = {
    loadPartitionsByBytes(path, bytesPerPartition) { (in, p, last) =>
      if (p > 0) StringUtil.readLine(in) // skip first line
      IteratorUtil.whileDefined {
        if (last || in.getCount <= bytesPerPartition) Option(StringUtil.readLine(in))
        else None
      }
    }
  }

  def loadPartitionsByBytes[A : ClassTag](path: String, bytesPerPartition: Long = 10.gb)(load: (CountingInputStream, Int, Boolean) => Iterator[A]): RDD[A] = {
    loadPartitions(path) { f =>
      val fileSize = HdfsIO.length(f)
      val uncompressedSize = if (f.toLowerCase.endsWith(GzipExt)) {
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

  def loadTextLinesGroupedByPrefix(path: String, prefix: String => String, readFully: Boolean = false, sorted: Boolean = false, strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy): RDD[(String, Iterator[String])] = {
    groupSorted(loadTextLines(path, readFully = readFully, sorted = sorted, strategy = strategy), prefix)
  }

  def doPartitions[A : ClassTag](rdd: RDD[A])(action: Int => Unit): RDD[A] = rdd.mapPartitionsWithIndex((idx, records) => {
    action(idx)
    records
  }, preservesPartitioning = true)

  def groupSorted[A, B](rdd: RDD[A], groupBy: A => B): RDD[(B, Iterator[A])] = rdd.mapPartitions(IteratorUtil.groupSorted(_, groupBy))

  def distinct[D : ClassTag](rdd: RDD[D], subtract: TraversableOnce[RDD[D]] = Seq.empty, partitions: Int = parallelism): RDD[D] = distinct(rdd, subtract, new HashPartitioner(partitions))

  def distinct[D : ClassTag](rdd: RDD[D], partitioner: Partitioner): RDD[D] = distinct(rdd, Seq.empty, partitioner)

  def distinct[D : ClassTag](rdd: RDD[D], subtract: TraversableOnce[RDD[D]], partitioner: Partitioner): RDD[D] = {
    var distinct = rdd.mapPartitions { records =>
      val distinct = records.toSet
      distinct.toIterator.map(s => (s, true))
    }.partitionBy(partitioner)

    for (s <- subtract) {
      distinct = distinct.subtract(s.mapPartitions { records =>
        val distinct = records.toSet
        distinct.toIterator.map(s => (s, true))
      }.partitionBy(partitioner), partitioner)
    }

    distinct.mapPartitions(_.map{case (record, _) => record}.toSet.toIterator)
  }

  def iterateDistinctPartitions[D : ClassTag](rdd: RDD[D], subtract: TraversableOnce[RDD[D]] = Seq.empty, partitions: Int = parallelism): Iterator[Set[D]] = {
    val partitioner = new HashPartitioner(partitions)
    val repartitioned = shuffle(distinct(rdd, subtract, partitioner), partitions)
    iteratePartitions(repartitioned).map(_.toSet)
  }

  def iterate[D : ClassTag](rdd: RDD[D], bufferSize: Int = 1000): CleanupIterator[D] = {
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

  def iterateAggregates[D : ClassTag, A : ClassTag](rdd: RDD[D], bufferSize: Int = 1000)(aggregate: Seq[D] => A): CleanupIterator[AggregateRecordsPointer[D, A]] = {
    iterate(rdd.mapPartitionsWithIndex { (idx, records) =>
      records.grouped(bufferSize).zipWithIndex.map { case (group, i) =>
        (aggregate(group), idx, i * bufferSize, bufferSize)
      }
    }, bufferSize).chain(_.map { case (v, p, o, l) =>
      AggregateRecordsPointer(v, RecordsPointer(rdd, p, o, l))
    })
  }

  def accessPartitionRange[D : ClassTag](rdd: RDD[D], pointer: RecordsPointer[D]): Array[D] = accessPartitionRange(rdd, pointer.partitionIdx, pointer.offset, pointer.length)

  def accessPartitionRange[D : ClassTag](rdd: RDD[D], partitionIdx: Int, offset: Int, length: Int): Array[D] = {
    sc.runJob(rdd, (iter: Iterator[D]) => iter.slice(offset, offset + length).toArray, Seq(partitionIdx)).head
  }

  def saveAsNamedTextFile(rdd: => RDD[(String,String)], path: String, partitions: Int = Sparkling.parallelism, repartition: Boolean = false, sorted: Boolean = false, skipIfExists: Boolean = false): Long = {
    val completeFlagFile = path + "/" + CompleteFlagFile
    if (skipIfExists && HdfsIO.exists(completeFlagFile)) return 0L
    HdfsIO.ensureOutDir(path)
    val repartitioned = if (repartition) rdd.partitionBy(new HashPartitioner(partitions)) else rdd
    val processed = repartitioned.mapPartitionsWithIndex { case (idx, records) =>
      val streams = collection.mutable.Map.empty[String, PrintStream]
      var prevFile: String = null
      var currentStream: PrintStream = null
      var processed: Long = 0L
      for ((file, value) <- records) {
        val filename = if (repartition || sorted) file else {
          val split = file.split("\\.", 2)
          val (filePrefix, fileExt) = (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
          filePrefix + "-" + StringUtil.padNum(idx, 5) + fileExt
        }
        val outPath = new Path(path, filename).toString
        val out = if (sorted) {
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
    }.reduce(_ + _)
    HdfsIO.touch(completeFlagFile)
    processed
  }

  def savePartitions[A](rdd: => RDD[A], path: String, skipIfExists: Boolean = false, checkPerFile: Boolean = false, skipEmpty: Boolean = true)(action: (Iterator[A], OutputStream) => Long): Long = {
    val completeFlagFile = path + "/" + CompleteFlagFile
    if (skipIfExists && HdfsIO.exists(completeFlagFile)) return 0L
    HdfsIO.ensureOutDir(path, ensureNew = !(skipIfExists && checkPerFile))
    val fileName = new Path(path).getName
    val split = fileName.split("\\.", 2)
    val (filePrefix, fileExt) = (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
    val processed = rdd.mapPartitionsWithIndex { (idx, records) =>
      val outPath = new Path(path, Sparkling.taskOutFile.getOrElse(filePrefix + "-" + StringUtil.padNum(idx, 5) + fileExt)).toString
      if (skipIfExists && HdfsIO.exists(outPath)) Iterator(0L)
      else {
        if (skipEmpty && !records.hasNext) Iterator(0L)
        else {
          val out = HdfsIO.out(outPath, overwrite = true)
          val processed = action(records, out)
          out.close()
          Iterator(processed)
        }
      }
    }.reduce(_ + _)
    HdfsIO.touch(completeFlagFile)
    processed
  }

  def saveTyped[A : TypedInOut](rdd: => RDD[A], path: String, skipIfExists: Boolean = false, checkPerFile: Boolean = false): Long = {
    val inout = implicitly[TypedInOut[A]]
    savePartitions(rdd, path, skipIfExists = skipIfExists, checkPerFile = checkPerFile) { (records, out) =>
      val writer = inout.out(new NonClosingOutputStream(out))
      Common.touch(records.map { r =>
        writer.write(r)
        1L
      }.sum)(_ => writer.close())
    }
  }

  def saveAsTextFile(rdd: => RDD[String], path: String, skipIfExists: Boolean = false, checkPerFile: Boolean = false): Long = {
    savePartitions(rdd, path, skipIfExists, checkPerFile) { (records, out) =>
      val print = IOUtil.print(out, closing = false)
      Common.touch(records.map { r =>
        print.println(r)
        1L
      }.sum)(_ => print.close())
    }
  }

  def saveSplits[A](rdd: => RDD[Iterator[A]], path: String, skipIfExists: Boolean = false)(action: (Iterator[A], OutputStream) => Long): Long = {
    val completeFlagFile = path + "/" + CompleteFlagFile
    if (skipIfExists && HdfsIO.exists(completeFlagFile)) return 0L
    HdfsIO.ensureOutDir(path)
    val pathFileName = new Path(path).getName
    val split = pathFileName.split("\\.", 2)
    val (pathFilePrefix, pathFileExt) = (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
    val processed = rdd.mapPartitionsWithIndex { (idx, splits) =>
      val (filePrefix, fileExt) = Sparkling.taskOutFile.map { fileName =>
        val split = fileName.split("\\.", 2)
        (split.head, split.drop(1).headOption.map("." + _).getOrElse(TxtExt))
      }.getOrElse((pathFilePrefix + "-" + StringUtil.padNum(idx, 5), pathFileExt))
      Iterator(splits.zipWithIndex.map { case (records, i) =>
        val outPath = new Path(path, filePrefix + "-" + StringUtil.padNum(i, 5) + fileExt).toString
        val out = HdfsIO.out(outPath, overwrite = true)
        val processed = action(records, out)
        out.close()
        processed
      }.sum)
    }.reduce(_ + _)
    HdfsIO.touch(completeFlagFile)
    processed
  }

  def saveTextSplits(rdd: => RDD[String], path: String, max: Long, length: String => Long, skipIfExists: Boolean = false): Long = {
    saveSplits(rdd.mapPartitions(records => IteratorUtil.grouped(records, max)(length)), path, skipIfExists) { (records, out) =>
      val print = IOUtil.print(out, closing = false)
      Common.touch(records.map { r =>
        print.println(r)
        1L
      }.sum)(_ => print.close())
    }
  }

  def saveTextSplitsByLines(rdd: => RDD[String], path: String, lines: Int = 1000000, skipIfExists: Boolean = false): Long = saveTextSplits(rdd, path, lines, _ => 1L, skipIfExists)

  def saveTextSplitsByBytes(rdd: => RDD[String], path: String, bytes: Long = 1.gb, skipIfExists: Boolean = false): Long = saveTextSplits(rdd, path, bytes, _.length, skipIfExists)

  def collectDistinct[A : ClassTag](rdd: RDD[A], minus: Set[A] = Set.empty[A]): Set[A] = {
    val bc = sc.broadcast(minus)
    rdd.mapPartitions(records => Iterator(records.toSet -- bc.value)).reduce(_ ++ _)
  }

  def sortByAndWithinPartitions[A : ClassTag, P : ClassTag : Ordering, S : ClassTag](rdd: RDD[A], partitions: Int = parallelism, ascending: Boolean = true)(by: A => (P, S))(implicit ordering: Ordering[(P,S)]): RDD[A] = {
    val withKey = rdd.keyBy(by)
    val partitioner = new PartialKeyRangePartitioner[(P, S), A, P](partitions, withKey, _._1, ascending)
    new ShuffledRDD[(P, S), A, A](withKey, partitioner).setKeyOrdering(if (ascending) ordering else ordering.reverse).values
  }

  def repartitionByAndSort[A : ClassTag, S : ClassTag : Ordering, P : ClassTag](rdd: RDD[(S, A)], partitions: Int = parallelism, ascending: Boolean = true)(by: S => P)(implicit ordering: Ordering[S]): RDD[(S, A)] = {
    val partitioner = new PartialKeyPartitioner[S, P](partitions, by)
    new ShuffledRDD[S, A, A](rdd, partitioner).setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  def repartitionByPrimaryAndSort[A : ClassTag, P : ClassTag, S : ClassTag](rdd: RDD[((P,S), A)], partitions: Int = parallelism, ascending: Boolean = true)(implicit ordering: Ordering[(P,S)]): RDD[((P,S), A)] = {
    val partitioner = new PrimaryKeyPartitioner(partitions)
    new ShuffledRDD[(P, S), A, A](rdd, partitioner).setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  def lazyFlatMap[A : ClassTag, B : ClassTag](rdd: RDD[A])(map: A => TraversableOnce[B]): RDD[B] = lazyMapPartitions(rdd) { (_, records) =>
    records.flatMap(map)
  }

  def lazyMapPartitions[A : ClassTag, B : ClassTag](rdd: RDD[A])(map: (Int, Iterator[A]) => Iterator[B]): RDD[B] = rdd.mapPartitionsWithIndex { (idx, records) =>
    IteratorUtil.lazyIter(map(idx, records))
  }

  private var cacheCounter = 0L

  def cache[A : ClassTag : TypedInOut](rdd: RDD[A]): RDD[A] = Sparkling.initPartitions {
    cacheCounter += 1
    val cacheCount = cacheCounter
    val inout = implicitly[TypedInOut[A]]
    lazyMapPartitions(rdd) { (idx, records) =>
      val dir = new File(Sparkling.LocalCacheDir).getCanonicalFile
      dir.mkdirs()
      val filename = "partition-" + StringUtil.padNum(cacheCount, 5) + "-" + StringUtil.padNum(idx, 5) + TmpExt + GzipExt
      val cacheFile = new File(dir, filename)
      if (!cacheFile.exists) {
        val tmpFile = IOUtil.tmpFile
        val out = inout.out(new GZIPOutputStream(new FileOutputStream(tmpFile)))
        records.foreach(out.write)
        out.close()
        Files.move(tmpFile, cacheFile)
      }
      val in = new GZIPInputStream(new FileInputStream(cacheFile))
      IteratorUtil.cleanup(inout.in(in), in.close)
    }
  }
}