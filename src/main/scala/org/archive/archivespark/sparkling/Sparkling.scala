package org.archive.archivespark.sparkling

import java.io.File
import java.util.concurrent.Executors

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}
import org.archive.archivespark.sparkling.cdx.CdxRecord
import org.archive.archivespark.sparkling.http.HttpMessage
import org.archive.archivespark.sparkling.io.{FileOutputPool, HdfsIO, TypedInOut}
import org.archive.archivespark.sparkling.util.{CollectionUtil, RddUtil, StringUtil}
import org.archive.archivespark.sparkling.warc.WarcRecord

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.reflect.ClassTag
import scala.util.Properties

object Sparkling {
  val Name: String = "Sparkling"
  val ScalaVersion: String = Properties.versionNumberString.split('.').take(2).mkString(".")
  val SparkVersion = "2.4.5"

  val GzipExt = ".gz"
  val WarcExt = ".warc"
  val ArcExt = ".arc"
  val CdxExt = ".cdx"
  val TxtExt = ".txt"
  val AttachmentExt = ".att"
  val CdxAttachmentExt = ".cdxa"
  val ErrLogExt = ".err"
  val TmpExt = ".tmp"
  val IdxExt = ".idx"

  val DefaultCharset = "UTF-8"

  val LocalCacheDir = "../sparkling-cache" // ../ for application directory instead of container, used by RddUtil
  val CompleteFlagFile = "_complete"

  implicit lazy val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))

  lazy val jarPath: String = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getPath

  private var _props: List[SparklingDistributedProp] = List.empty

  def prop[A](value: A)(get: => A, set: A => Unit): A = {
    _props +:= new SparklingDistributedProp(get, v => set(v.asInstanceOf[A]))
    value
  }

  var minOpenFiles: Int = prop(100)(minOpenFiles, minOpenFiles = _)
  var maxOpenFiles: Int = prop(300)(maxOpenFiles, maxOpenFiles = _)
  private lazy val _fileOutPool = new ThreadLocal[FileOutputPool]
  def fileOutPool: FileOutputPool = {
    var value = _fileOutPool.get()
    if (value == null) {
      value = new FileOutputPool(minOpenFiles, maxOpenFiles)
      _fileOutPool.set(value)
    }
    value
  }

  var tmpHdfsPath: String = prop("/tmp")(tmpHdfsPath, tmpHdfsPath = _)
  var tmpFilePrefix: String = prop("sparkling-")(tmpFilePrefix, tmpFilePrefix = _)
  var tmpFileReplication: Short = prop(1.toShort)(tmpFileReplication, tmpFileReplication = _)
  var defaultReplication: Short = prop(2.toShort)(defaultReplication, defaultReplication = _)

  private[sparkling] val parentTaskContext = new ThreadLocal[TaskContext]
  def taskContext: Option[TaskContext] = Option(TaskContext.get).orElse(Option(parentTaskContext.get))

  def isLocal: Boolean = taskContext.isEmpty
  def taskId: Long = taskContext.map(_.taskAttemptId).getOrElse(0)
  def loggingId: String = StringUtil.padNum(taskId, 5)

  private val _taskStore = CollectionUtil.concurrentMap[Long, mutable.Map[String, Any]]
  def taskStore: mutable.Map[String, Any] = _taskStore.getOrElseUpdate(taskId, CollectionUtil.concurrentMap[String, Any])

  private val _taskInFiles = CollectionUtil.concurrentMap[Long, (Int, String)]
  def setTaskInFile(idx: Int, filename: String): String = {
    _taskInFiles.update(taskId, (idx, filename))
    filename
  }

  def taskInFile: Option[(Int, String)] = _taskInFiles.get(taskId)

  private val _taskOutFiles = CollectionUtil.concurrentMap[Long, String]
  def setTaskOutFile(filename: Option[String]): Unit = {
    val id = taskId
    filename match {
      case Some(f) => _taskOutFiles.update(id, f)
      case None    => _taskOutFiles.remove(id)
    }
  }

  def taskOutFile: Option[String] = _taskOutFiles.get(taskId)

  def setTaskOutFiles[A: ClassTag](rdd: RDD[A])(filename: (Int, Option[String]) => Option[String]): RDD[A] = {
    RddUtil.doPartitions(rdd)(idx => setTaskOutFile(filename(taskInFile.map(_._1).getOrElse(idx), taskInFile.map(_._2))))
  }

  def getTaskOutFile(map: Int => String): String = taskOutFile.getOrElse { taskInFile.map { case (idx, _) => map(idx) }.getOrElse(map(taskContext.map(_.partitionId).getOrElse(0))) }

  private var _sc: Option[SparkContext] = None

  def sc: SparkContext = _sc.filter(!_.isStopped) match {
    case Some(sc) => sc
    case None =>
      val sc = SparkContext.getOrCreate
      sc.getConf.registerKryoClasses(Array(
        classOf[CdxRecord],
        classOf[WarcRecord],
        classOf[HttpMessage],
        classOf[SparklingDistributedProp],
        classOf[TypedInOut[_]]
        // TODO: put these in their corresponding packages, see commented out registerKryoClasses(...) calls
//        classOf[PositionPointer],
//        classOf[RecordPointer],
//        classOf[SortedPrefixSeq],
//        classOf[BasicSortedPrefixSeq],
//        classOf[HdfsSortedPrefixSeq],
//        classOf[PartialDoc],
//        classOf[TokenFreqBasedSeq],
//        classOf[AggregatedDoc],
//        classOf[Attachment[_]]
      ))
      _sc = Some(sc)
      sc
  }

  private var _parallelism: Option[Int] = None
  def parallelism: Int = _parallelism.getOrElse(sc.defaultParallelism * 5)
  def parallelism_=(value: Int): Unit = _parallelism = Some(value)

  def broadcastProps: Broadcast[List[SparklingDistributedProp]] = {
    _props.foreach(_.save())
    sc.broadcast(_props)
  }

  def initPartitions[A: ClassTag](rdd: RDD[A]): RDD[A] = {
    val props = broadcastProps
    RddUtil.doPartitions(rdd)(_ => setProps(props))
  }

  def setProps(props: Broadcast[List[SparklingDistributedProp]]): Unit = setProps(props.value)
  def setProps(props: List[SparklingDistributedProp]): Unit = {
    _props = props
    _props.foreach(_.restore())
  }

  var autoClearCheckpoints: Boolean = true
  private var _checkpointDir: Option[String] = None
  private var _checkpointPrefix: String = ""

  def labelCheckpoints(label: String): Unit = _checkpointPrefix = if (label.trim.nonEmpty) label.trim + "_" else ""

  def checkpointDir(id: String): Option[String] = _checkpointDir.map(_ + "/" + _checkpointPrefix + id + "_checkpoint" + GzipExt)

  def enableCheckpointing(dir: String): Unit = {
    _checkpointDir = Some(dir)
    println("Checkpointing enabled (" + dir + ")")
  }

  def enableCheckpointing(): Unit = enableCheckpointing(HdfsIO.createTmpPath())

  def clearCheckpointDir(id: String): Unit = clearCheckpointDir(Some(id))
  def clearCheckpointDir(id: Option[String] = None): Unit = if (_checkpointDir.isDefined) { HdfsIO.delete(if (id.isDefined) checkpointDir(id.get).get else _checkpointDir.get) }

  def disableCheckpointing(clear: Boolean = true): Unit = {
    if (clear) clearCheckpointDir()
    _checkpointDir = None
  }

  def checkpointStrings[A: ClassTag](id: String, rdd: => RDD[A], skipIfExists: Boolean = true, checkPerFile: Boolean = true, readFully: Boolean = false)(
      toString: A => String,
      fromString: String => A
  ): RDD[A] = {
    implicit val inout: TypedInOut[A] = TypedInOut.toStringInOut(toString, fromString)
    checkpoint(id, skipIfExists, checkPerFile, readFully)(rdd)
  }

  def checkpoint[A: ClassTag: TypedInOut](id: String, skipIfExists: Boolean = true, checkPerFile: Boolean = true, readFully: Boolean = false)(rdd: => RDD[A]): RDD[A] = {
    if (_checkpointDir.isDefined) {
      val path = checkpointDir(id).get
      println("Checkpointing " + id + " (" + path + ")")
      RddUtil.saveTyped(rdd, path, skipIfExists = skipIfExists, checkPerFile = checkPerFile)
      RddUtil.loadTyped(path, readFully = readFully)
    } else rdd
  }

  def checkpointOrTmpDir[A](id: String)(action: String => A): A = checkpointDir(id) match {
    case Some(dir) =>
      val result = action(dir)
      if (autoClearCheckpoints) clearCheckpointDir(id)
      result
    case None => HdfsIO.tmpPath(action)
  }

  def getCheckpointOrTmpDir[A](id: String): String = checkpointDir(id) match {
    case Some(dir) => dir
    case None      => HdfsIO.createTmpPath()
  }
}
