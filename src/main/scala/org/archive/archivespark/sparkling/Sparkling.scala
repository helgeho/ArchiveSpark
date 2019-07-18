/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.archivespark.sparkling

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}
import org.archive.archivespark.sparkling.cdx.CdxRecord
import org.archive.archivespark.sparkling.http.HttpMessage
import org.archive.archivespark.sparkling.io.{HdfsIO, TypedInOut}
import org.archive.archivespark.sparkling.util.{RddUtil, StringUtil}
import org.archive.archivespark.sparkling.warc.WarcRecord

import scala.reflect.ClassTag

object Sparkling {
  val GzipExt = ".gz"
  val WarcExt = ".warc"
  val ArcExt = ".arc"
  val CdxExt = ".cdx"
  val TxtExt = ".txt"
  val AttachmentExt = ".att"
  val CdxAttachmentExt = ".cdxa"
  val ErrLogExt = ".err"
  val TmpExt = ".tmp"

  val DefaultCharset = "UTF-8"

  val LocalCacheDir = "../sparkling-cache" // ../ for application directory instead of container, used by RddUtil
  val CompleteFlagFile = "_complete"

  private var _props: List[SparklingDistributedProp] = List.empty

  def prop[A](value: A)(get: => A, set: A => Unit): A = {
    _props +:= new SparklingDistributedProp(get, v => set(v.asInstanceOf[A]))
    value
  }

  var tmpHdfsPath: String = prop("/tmp")(tmpHdfsPath, tmpHdfsPath = _)
  var tmpFilePrefix: String = prop("sparkling-")(tmpFilePrefix, tmpFilePrefix = _)
  var tmpFileReplication: Short = prop(2.toShort)(tmpFileReplication, tmpFileReplication = _)

  def loggingId: String = StringUtil.padNum(if (isLocal) 0 else TaskContext.get.taskAttemptId, 5)
  def isLocal: Boolean = TaskContext.getPartitionId == 0

  def taskId: Long = Option(TaskContext.get).map(_.taskAttemptId).getOrElse(0)

  private val _taskStore = collection.mutable.Map.empty[Long, collection.mutable.Map[String, Any]]
  def taskStore: collection.mutable.Map[String, Any] = _taskStore.getOrElseUpdate(taskId, collection.mutable.Map.empty)

  private val _taskInFiles = collection.mutable.Map.empty[Long, Option[String]]
  def setTaskInFile(filename: String): String = {
    _taskInFiles.update(taskId, Some(filename))
    filename
  }

  def taskInFile: Option[String] = _taskInFiles.getOrElse(taskId, None)

  private val _taskOutFiles = collection.mutable.Map.empty[Long, String]
  def setTaskOutFile(filename: Option[String]): Unit = {
    val id = taskId
    filename match {
      case Some(f) => _taskOutFiles.update(id, f)
      case None => _taskOutFiles.remove(id)
    }
  }

  def taskOutFile: Option[String] = _taskOutFiles.get(taskId)

  def setOutFiles[A : ClassTag](rdd: RDD[A])(filename: (Int, Option[String]) => Option[String]): RDD[A] = {
    RddUtil.doPartitions(rdd)(idx => setTaskOutFile(filename(idx, taskInFile)))
  }

  lazy val sc: SparkContext = {
    val sc = SparkContext.getOrCreate
    sc.getConf.registerKryoClasses(Array(
      classOf[CdxRecord],
      classOf[WarcRecord],
      classOf[HttpMessage],
      classOf[SparklingDistributedProp],
      classOf[TypedInOut[_]]
    ))
    sc
  }

  private var _parallelism: Option[Int] = None
  def parallelism: Int = _parallelism.getOrElse(sc.defaultParallelism * 5)
  def parallelism_=(value: Int): Unit = _parallelism = Some(value)

  def broadcastProps: Broadcast[List[SparklingDistributedProp]] = {
    _props.foreach(_.save())
    sc.broadcast(_props)
  }

  def initPartitions[A : ClassTag](rdd: RDD[A]): RDD[A] = {
    val props = broadcastProps
    RddUtil.doPartitions(rdd)(_ => setProps(props))
  }

  def setProps(props: Broadcast[List[SparklingDistributedProp]]): Unit = setProps(props.value)
  def setProps(props: List[SparklingDistributedProp]): Unit = {
    _props = props
    _props.foreach(_.restore())
  }

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
  def clearCheckpointDir(id: Option[String] = None): Unit = if (_checkpointDir.isDefined) {
    HdfsIO.delete(if (id.isDefined) checkpointDir(id.get).get else _checkpointDir.get)
  }

  def disableCheckpointing(clear: Boolean = true): Unit = {
    if (clear) clearCheckpointDir()
    _checkpointDir = None
  }

  def checkpointStrings[A : ClassTag](id: String, rdd: => RDD[A], skipIfExists: Boolean = true, checkPerFile: Boolean = true, readFully: Boolean = false)(toString: A => String, fromString: String => A): RDD[A] = {
    implicit val inout: TypedInOut[A] = TypedInOut.toStringInOut(toString, fromString)
    checkpoint(id, skipIfExists, checkPerFile, readFully)(rdd)
  }

  def checkpoint[A : ClassTag : TypedInOut](id: String, skipIfExists: Boolean = true, checkPerFile: Boolean = true, readFully: Boolean = false)(rdd: => RDD[A]): RDD[A] = {
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
      clearCheckpointDir(id)
      result
    case None => HdfsIO.tmpPath(action)
  }

  def getCheckpointOrTmpDir[A](id: String): String = checkpointDir(id) match {
    case Some(dir) => dir
    case None => HdfsIO.createTmpPath()
  }
}
