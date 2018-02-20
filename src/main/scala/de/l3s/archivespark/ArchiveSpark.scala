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

package de.l3s.archivespark

import de.l3s.archivespark.dataspecs.DataSpec
import de.l3s.archivespark.dataspecs.access._
import de.l3s.archivespark.enrich.dataloads.DataLoadBase
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.enrich.{EnrichRoot, _}
import de.l3s.archivespark.specific.warc.specs.{CdxHdfsSpec, WarcCdxHdfsSpec}
import de.l3s.archivespark.specific.warc.{CdxRecord, WarcRecord}
import de.l3s.archivespark.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

import scala.reflect.ClassTag

object ArchiveSpark {
  val AppName = "ArchiveSpark"
  val Namespace = "de.l3s.archivespark"

  def prop(key: String): String = if (key.startsWith(Namespace)) key else s"$Namespace.$key"
  def setProp(conf: SparkConf, key: String, value: Any): Unit = conf.set(prop(key), value.toString)

  object props {
    val initialized: String = prop("initialized")
  }

  var parallelism = 0

  def partitions: Int = partitions(SparkContext.getOrCreate)
  def partitions(sc: SparkContext): Int = if (parallelism > 0) parallelism else sc.defaultParallelism

  var conf: DistributedConfig = new DistributedConfig()

  def initialize(sc: SparkContext): Unit = initialize(sc.getConf)
  def initialize(conf: SparkConf): Unit = {
    if (conf.getBoolean(props.initialized, defaultValue = false)) return
    setProp(conf, props.initialized, true)
    conf.setAppName(AppName)
    conf.registerKryoClasses(Array(
      classOf[DistributedConfig],
      classOf[DataSpec[_, _]],
      classOf[DataAccessor[_]],
      classOf[CloseableDataAccessor[_]],
      classOf[HdfsLocationInfo],
      classOf[HdfsStreamAccessor],
      classOf[HdfsTextAccessor],
      classOf[HttpTextAccessor],
      classOf[DataLoadBase],
      classOf[Enrichable],
      classOf[EnrichRoot],
      classOf[EnrichFunc[_, _]],
      classOf[EnrichFuncWithDefaultField[_, _, _, _]],
      classOf[BoundEnrichFunc[_, _]],
      classOf[BoundEnrichFuncWithDefaultField[_, _, _]],
      classOf[DependentEnrichFunc[_, _]],
      classOf[DependentEnrichFuncWithDefaultField[_, _, _, _]],
      classOf[JsonConvertible],
      classOf[Copyable[_]],
      classOf[SingleValueEnrichable[_]],
      classOf[MultiValueEnrichable[_]],
      classOf[BasicEnrichFunc[_, _, _]],
      classOf[BasicDependentEnrichFunc[_, _, _]],
      classOf[BasicMultiValEnrichFunc[_, _, _]],
      classOf[BasicMultiValDependentEnrichFunc[_, _, _]],
      classOf[DefaultFieldAccess[_, _]],
      classOf[DefaultField[_]],
      classOf[SingleField[_]],
      classOf[Derivatives],
      classOf[IdentityEnrichFunction[_, _]],
      classOf[IdentityField[_]],
      classOf[MultiVal],
      classOf[MultiValueEnrichable[_]],
      classOf[PipedDependentEnrichFunc[_, _]],
      classOf[PipedEnrichFunc[_]],
      classOf[RootEnrichFunc[_]],
      classOf[SingleValueEnrichable[_]],
      classOf[Data[_]], classOf[DataLoad[_]], classOf[Entities], classOf[HtmlTag], classOf[HtmlTags], classOf[HtmlAttribute], classOf[Json],
      classOf[Root[_]], classOf[Values]
    ))
  }

  def load[Raw, Parsed : ClassTag](spec: DataSpec[Raw, Parsed]): RDD[Parsed] = load(SparkContext.getOrCreate, spec)

  def load[Raw, Parsed : ClassTag](sc: SparkContext, spec: DataSpec[Raw, Parsed]): RDD[Parsed] = {
    initialize(sc)
    spec.initialize(sc)
    val raw = spec.load(sc, partitions(sc))
    val specBc = sc.broadcast(spec)
    raw.mapPartitions{records =>
      val spec = specBc.value
      records.flatMap(spec.parse)
    }
  }

  def hdfs(cdxPath: String, warcPath: String)(implicit sc: SparkContext = SparkContext.getOrCreate): RDD[WarcRecord] = load(sc, WarcCdxHdfsSpec(cdxPath, warcPath))

  def cdx(path: String)(implicit sc: SparkContext = SparkContext.getOrCreate): RDD[CdxRecord] = load(sc, CdxHdfsSpec(path))
}
