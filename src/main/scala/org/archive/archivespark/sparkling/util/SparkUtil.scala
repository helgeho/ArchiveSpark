package org.archive.archivespark.sparkling.util

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, TaskContext}
import org.archive.archivespark.sparkling.io.HdfsIO

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.archive.archivespark.sparkling.Sparkling
import org.apache.log4j._
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}

object SparkUtil {
  import org.archive.archivespark.sparkling.Sparkling._

  trait SparkReplClassServer {
    def stop(): Unit
    def uri: String
  }

  val DefaultConfig: Map[String, String] = Map(
    "spark.master" -> "yarn",
    "spark.submit.deployMode" -> "client",
    "spark.network.timeout" -> "9999999s",
    "spark.executor.heartbeatInterval" -> "999999s",
    "spark.driver.port" -> "54728",
    "spark.ui.port" -> "54040",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.executorEnv.JAVA_HOME" -> "/usr/lib/jvm/jdk1.8.0_144",
    "spark.sql.constraintPropagation.enabled" -> "false",
    "spark.default.parallelism" -> "100"
  )

  private var _currentContext: Option[SparkContext] = None
  def currentContext: Option[SparkContext] = _currentContext

  // interp.load.cp(ammonite.ops.Path(sys.env.get("HADOOP_CONF_DIR").get))
  // Thread.currentThread().getContextClassLoader.getResource("core-site.xml")
  // https://github.com/alexarchambault/ammonite-spark/blob/b33e9145fda4a13d9020cef13bb461e65634ae39/modules/core/src/main/scala/org/apache/spark/sql/ammonitesparkinternals/AmmoniteSparkSessionBuilder.scala

  // to use Almond / Ammonite builder: org.apache.spark.sql.NotebookSparkSession.builder().progress(false)
  def config[B <: SparkSession.Builder](
      builder: B = SparkSession.builder,
      appName: String = "",
      queue: String = "default",
      executors: Int = 100,
      executorCores: Int = 1,
      executorMemory: String = "2g",
      additionalConfigs: Map[String, String] = Map.empty,
      verbose: Boolean = false,
      jars: Seq[String] = Seq.empty,
      replClassServer: SparkReplClassServer = null
  ): SparkContext = {
    for (sc <- currentContext if !sc.isStopped) sc.stop()
    if (!verbose) {
      Logger.getRootLogger.setLevel(Level.OFF)
      Logger.getLogger("org").setLevel(Level.OFF)
    }
    var building = builder.appName(Some(appName).filter(_.nonEmpty).map(_ + " (" + Sparkling.Name + ")").getOrElse(Sparkling.Name))
    val config = DefaultConfig ++ Map("spark.yarn.queue" -> queue, "spark.executor.instances" -> executors, "spark.executor.cores" -> executorCores, "spark.executor.memory" -> executorMemory) ++
      Option(replClassServer).map("spark.repl.class.uri" -> _.uri).toMap ++ additionalConfigs
    for ((k, v) <- config) building = building.config(k, v.toString)
    val sc = building.getOrCreate.sparkContext
    _currentContext = Some(sc)
    for (p <- Seq(jarPath) ++ jars) sc.addJar(p)
    for (server <- Option(replClassServer)) sc.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = server.stop()
    })
    sc
  }

  def dynamicAllocationConfig[B <: SparkSession.Builder](
      builder: B = SparkSession.builder,
      appName: String = "",
      queue: String = "default",
      minExecutors: Int = 2,
      maxExecutors: Int = 20,
      executorCores: Int = 1,
      executorMemory: String = "2g",
      executorIdleTimeout: String = "60s",
      additionalConfigs: Map[String, String] = Map.empty,
      verbose: Boolean = false,
      jars: Seq[String] = Seq.empty,
      replClassServer: SparkReplClassServer = null
  ): SparkContext = {
    for (sc <- currentContext if !sc.isStopped) sc.stop()
    if (!verbose) {
      Logger.getRootLogger.setLevel(Level.OFF)
      Logger.getLogger("org").setLevel(Level.OFF)
    }
    var building = builder.appName(Some(appName).filter(_.nonEmpty).map(_ + " (" + Sparkling.Name + ")").getOrElse(Sparkling.Name) + " [dynamic]")
    val config = DefaultConfig ++ Map(
      "spark.yarn.queue" -> queue,
      "spark.executor.cores" -> executorCores,
      "spark.executor.memory" -> executorMemory,
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.shuffle.service.enabled" -> "true",
      "spark.dynamicAllocation.minExecutors" -> minExecutors,
      "spark.dynamicAllocation.maxExecutors" -> maxExecutors,
      "spark.dynamicAllocation.executorIdleTimeout" -> executorIdleTimeout
    ) ++ Option(replClassServer).map("spark.repl.class.uri" -> _.uri).toMap ++ additionalConfigs
    for ((k, v) <- config) building = building.config(k, v.toString)
    val sc = building.getOrCreate.sparkContext
    _currentContext = Some(sc)
    for (p <- Seq(jarPath) ++ jars) sc.addJar(p)
    for (server <- Option(replClassServer)) sc.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = server.stop()
    })
    sc
  }

  def addLocalFile(path: String): Unit = {
    val src = new Path(path)
    val dst = new Path(HdfsIO.createTmpPath(), src.getName)
    HdfsIO.fs.copyFromLocalFile(src, dst)
    sc.addFile("hdfs://" + dst.toString, recursive = true)
  }

  private var cleanupObjects = CollectionUtil.concurrentMap[Any, Long]
  private var cleanups = CollectionUtil.concurrentMap[Long, mutable.Map[Any, () => Unit]]

  def cleanupTask(owner: Any, cleanup: () => Unit): Unit = {
    val task = TaskContext.get
    if (task != null) {
      cleanupObjects.getOrElseUpdate(
        owner, {
          val attemptId = task.taskAttemptId
          val taskCleanups = cleanups.getOrElseUpdate(
            attemptId, {
              val taskCleanups = CollectionUtil.concurrentMap[Any, () => Unit]
              task.addTaskCompletionListener[Unit] { ctx: TaskContext =>
                cleanups.remove(attemptId)
                for ((o, c) <- taskCleanups) {
                  cleanupObjects.remove(o)
                  c()
                }
                taskCleanups.clear()
              }
              taskCleanups
            }
          )
          taskCleanups.update(owner, cleanup)
          attemptId
        }
      )
    }
  }

  def removeTaskCleanup(owner: Any): Unit = { for (attemptId <- cleanupObjects.remove(owner); attempt <- cleanups.get(attemptId)) attempt.remove(owner) }
}
