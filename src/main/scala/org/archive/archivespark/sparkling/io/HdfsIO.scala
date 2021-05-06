package org.archive.archivespark.sparkling.io

import java.io.{FileSystem => _, _}
import java.util.zip.GZIPOutputStream

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs._
import org.apache.spark.deploy.SparkHadoopUtil
import org.archive.archivespark.sparkling.logging.{Log, LogContext}
import org.archive.archivespark.sparkling.util.{CleanupIterator, Common, IteratorUtil}

import scala.util.{Random, Try}

object HdfsIO {
  implicit val logContext: LogContext = LogContext(this)

  val DefaultLineBuffer = 1000
  val ReplicationProperty = "dfs.replication"
  val BufferSizeProperty = "io.file.buffer.size"

  import org.archive.archivespark.sparkling.Sparkling._

  def fs: FileSystem = FileSystem.get(SparkHadoopUtil.get.conf)

  object LoadingStrategy extends Enumeration {
    val Remote, BlockWise, CopyLocal, Dynamic = Value
  }

  type LoadingStrategy = LoadingStrategy.Value

  var defaultLoadingStrategy: LoadingStrategy = prop(LoadingStrategy.Dynamic)(defaultLoadingStrategy, defaultLoadingStrategy = _)
  var dynamicCopyLocalThreshold: Double = prop(0.5)(dynamicCopyLocalThreshold, dynamicCopyLocalThreshold = _)
  var blockReadTimeoutMillis: Int = prop(1000 * 60 * 5)(blockReadTimeoutMillis, blockReadTimeoutMillis = _) // 5 minutes

  private var localFiles: Map[String, String] = Map.empty

  def open(
      path: String,
      offset: Long = 0,
      length: Long = 0,
      decompress: Boolean = true,
      retries: Int = 10,
      sleepMillis: Int = 1000,
      strategy: LoadingStrategy = defaultLoadingStrategy
  ): InputStream = {
    val loadingStrategy =
      if (strategy == LoadingStrategy.Dynamic) {
        val fileSize = HdfsIO.length(path)
        val copyLocalThreshold = fileSize.toDouble * dynamicCopyLocalThreshold
        if (localFiles.contains(path)) LoadingStrategy.CopyLocal
        else if (offset < copyLocalThreshold && (length < 0 || length > copyLocalThreshold)) LoadingStrategy.CopyLocal
        else LoadingStrategy.BlockWise
      } else strategy
    Log.info(
      "Opening file " + path + " (Offset: " + offset + ", length: " + length + ", decompress: " + decompress + ", strategy: " + loadingStrategy +
        (if (strategy == LoadingStrategy.Dynamic) " [dynamic]" else "") + ")"
    )
    val in = loadingStrategy match {
      case LoadingStrategy.Remote => Common.retryObj(fs.open(new Path(path)))(
          retries,
          sleepMillis,
          _.close,
          (_, retry, e) => {
            "File access failed (" + retry + "/" + retries + "): " + path + " (Offset: " + offset + ")" + Option(e.getMessage).map(_.trim).filter(_.nonEmpty).map(" - " + _).getOrElse("")
          }
        ) { (in, retry) =>
          if (retry > 0) in.seekToNewSource(offset) else if (offset > 0) in.seek(offset)
          val buffered = if (length > 0) new BufferedInputStream(new BoundedInputStream(in, length)) else new BufferedInputStream(in)
          if (IOUtil.eof(buffered)) {
            buffered.close()
            IOUtil.EmptyStream
          } else buffered
        }
      case LoadingStrategy.BlockWise => new BufferedInputStream(new HdfsBlockStream(fs, path, offset, length, retries, sleepMillis, blockReadTimeoutMillis))
      case LoadingStrategy.CopyLocal => Common.retryObj {
          localFiles = localFiles.synchronized(localFiles.updated(
            path,
            localFiles.getOrElse(
              path, {
                val tmpPath = IOUtil.tmpFile.getCanonicalPath
                fs.copyToLocalFile(new Path(path), new Path(tmpPath))
                tmpPath
              }
            )
          ))
          new FileInputStream(localFiles(path))
        }(
          retries,
          sleepMillis,
          _.close,
          (_, retry, e) => { "File access failed (" + retry + "/" + retries + "): " + path + Option(e.getMessage).map(_.trim).filter(_.nonEmpty).map(" - " + _).getOrElse("") }
        ) { (in, retry) =>
          if (offset > 0) in.getChannel.position(offset)
          val buffered = if (length > 0) new BufferedInputStream(new BoundedInputStream(in, length)) else new BufferedInputStream(in)
          if (IOUtil.eof(buffered)) {
            buffered.close()
            IOUtil.EmptyStream
          } else buffered
        }
    }
    if (decompress) IOUtil.decompress(in, Some(path)) else in
  }

  def access[R](path: String, offset: Long = 0, length: Long = 0, decompress: Boolean = true, retries: Int = 60, sleepMillis: Int = 1000 * 60, strategy: LoadingStrategy = defaultLoadingStrategy)(
      action: InputStream => R
  ): R = {
    val in = open(path, offset, length, decompress, retries, sleepMillis, strategy)
    val r = action(in)
    Try(in.close())
    r
  }

  def copyFromLocal(src: String, dst: String, move: Boolean = false, overwrite: Boolean = false, replication: Short = 0): Unit = {
    val dstTmpPath = new Path(dst + "._copying")
    if (overwrite) delete(dst)
    val dstPath = new Path(dst)
    val dstReplication = if (replication == 0) if (defaultReplication == 0) fs.getDefaultReplication(dstPath) else defaultReplication else replication
    val conf = new org.apache.hadoop.conf.Configuration(SparkHadoopUtil.get.conf)
    conf.setInt(ReplicationProperty, 1)
    FileUtil.copy(FileSystem.getLocal(conf), new Path(src), fs, dstTmpPath, move, true, conf)
    fs.rename(dstTmpPath, dstPath)
    if (dstReplication > 1) fs.setReplication(dstPath, dstReplication)
  }

  def rename(src: String, dst: String): Unit = fs.rename(new Path(src), new Path(dst))

  def length(path: String): Long = HdfsIO.fs.getFileStatus(new Path(path)).getLen

  def lines(path: String, n: Int = -1, offset: Long = 0): Seq[String] = access(path, offset, length = if (n < 0) -1 else 0) { in =>
    val lines = IOUtil.lines(in)
    if (n < 0) lines.toList else lines.take(n).toList
  }

  def files(path: String, recursive: Boolean = true): Iterator[String] = {
    val glob = fs.globStatus(new Path(path))
    if (glob == null) Iterator.empty
    else glob.toIterator.flatMap { status => if (status.isDirectory && recursive) files(new Path(status.getPath, "*").toString) else Iterator(status.getPath.toString) }
  }

  def dir(path: String): String = {
    val p = new Path(path)
    val status = fs.globStatus(p)
    if (status == null || status.isEmpty || (status.length == 1 && status.head.isDirectory)) path else p.getParent.toString
  }

  def createTmpPath(prefix: String = tmpFilePrefix, path: String = tmpHdfsPath, deleteOnExit: Boolean = true): String = {
    var rnd = System.currentTimeMillis + "-" + Random.nextInt.abs
    var tmpPath: Path = null
    while ({
      tmpPath = new Path(path, prefix + rnd)
      fs.exists(tmpPath) || !fs.mkdirs(tmpPath)
    }) rnd = System.currentTimeMillis + "-" + Random.nextInt.abs
    if (deleteOnExit) fs.deleteOnExit(tmpPath)
    tmpPath.toString
  }

  def tmpPath[R](action: String => R): R = {
    val path = createTmpPath()
    val r = action(path)
    delete(path)
    r
  }

  def delete(path: String): Unit = if (exists(path)) {
    val p = new Path(path)
    val success = fs.delete(p, true)
    if (!success) fs.deleteOnExit(p)
  }

  def exists(path: String): Boolean = fs.exists(new Path(path))

  def ensureOutDir(path: String, ensureNew: Boolean = true): Unit = {
    if (ensureNew && exists(path)) Common.printThrow("Path exists: " + path)
    fs.mkdirs(new Path(path))
  }

  def ensureNewFile(path: String): Unit = { if (exists(path)) Common.printThrow("File exists: " + path) }

  def writer(path: String, overwrite: Boolean = false, append: Boolean = false, replication: Short = 0): HdfsFileWriter = HdfsFileWriter(path, overwrite, append, replication)

  def bufferSize: Int = fs.getConf().getInt(BufferSizeProperty, 4096)

  def out(path: String, overwrite: Boolean = false, compress: Boolean = true, useWriter: Boolean = true, append: Boolean = false, temporary: Boolean = false): OutputStream = {
    val out =
      if (useWriter) writer(path, overwrite, append, if (temporary) tmpFileReplication else 0)
      else if (append) fs.append(new Path(path))
      else {
        val fsPath = new Path(path)
        if (temporary) fs.create(fsPath, overwrite, bufferSize, tmpFileReplication, fs.getDefaultBlockSize(fsPath)) else fs.create(fsPath, overwrite)
      }
    if (compress && path.toLowerCase.endsWith(GzipExt)) new GZIPOutputStream(out) else out
  }

  def writeLines(path: String, lines: => TraversableOnce[String], overwrite: Boolean = false, compress: Boolean = true, useWriter: Boolean = true, skipIfExists: Boolean = false): Long = {
    if (skipIfExists && exists(path)) 0L
    val stream = out(path, overwrite, compress, useWriter)
    val processed = IOUtil.writeLines(stream, lines)
    Try(stream.close())
    processed
  }

  def concat(files: Seq[String], outPath: String, append: Boolean = false): Unit = {
    val stream = out(outPath, compress = false, append = append)
    for (file <- files) HdfsIO.access(file, decompress = false) { in => IOUtil.copy(in, stream) }
    Try(stream.close())
  }

  def iterLines(path: String, readFully: Boolean = false): CleanupIterator[String] = CleanupIterator.flatten {
    HdfsIO.files(path).map { file =>
      val in = if (readFully) open(file, length = -1) else open(file)
      IteratorUtil.cleanup(IOUtil.lines(in), in.close)
    }
  }

  def countLines(path: String): Long = IteratorUtil.count(iterLines(path))

  def collectLines(path: String): Seq[String] = HdfsIO.files(path).toSeq.par.flatMap { file =>
    val in = open(file)
    IteratorUtil.cleanup(IOUtil.lines(in), in.close)
  }.seq

  def collectDistinctLines(path: String, parallel: Boolean = true, map: String => Option[String] = Some(_), lineBuffer: Int = DefaultLineBuffer): Set[String] =
    if (parallel) {
      val parallel = HdfsIO.files(path).toSet.par
      parallel.flatMap { file =>
        val in = open(file)
        IteratorUtil.cleanup(IOUtil.lines(in), in.close).flatMap(l => map(l)).grouped(lineBuffer).map(_.toSet).foldLeft(Set.empty[String])(_ ++ _)
      }.seq
    } else iterLines(path).flatMap(l => map(l)).grouped(lineBuffer).map(_.toSet).foldLeft(Set.empty[String])(_ ++ _)

  def touch(path: String): Unit = Common.touch(out(path, useWriter = false, append = exists(path), compress = false))(_.write(Array.empty[Byte])).close()
}
