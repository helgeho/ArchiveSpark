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

package org.archive.archivespark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.archive.archivespark.dataspecs.DataSpec
import org.archive.archivespark.dataspecs.access._
import org.archive.archivespark.functions._
import org.archive.archivespark.model.pointers._
import org.archive.archivespark.model.{EnrichRoot, _}
import org.archive.archivespark.sparkling.Sparkling
import org.archive.archivespark.util._

import scala.reflect.ClassTag

object ArchiveSpark {
  val AppName = "ArchiveSpark"
  val Namespace = "org.archive.archivespark"

  def prop(key: String): String = if (key.startsWith(Namespace)) key else s"$Namespace.$key"
  def setProp(conf: SparkConf, key: String, value: Any): Unit = conf.set(prop(key), value.toString)

  object props {
    val initialized: String = prop("initialized")
  }

  var conf: DistributedConfig = Sparkling.prop(new DistributedConfig())(conf, conf = _)

  def initialize(): Unit = {
    val conf = SparkContext.getOrCreate.getConf
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
      classOf[Enrichable],
      classOf[EnrichRoot],
      classOf[EnrichFunc[_, _, _]],
      classOf[MultiEnrichFunc[_, _, _]],
      classOf[BoundEnrichFunc[_, _, _]],
      classOf[BoundMultiEnrichFunc[_, _, _]],
      classOf[JsonConvertible],
      classOf[Copyable[_]],
      classOf[SingleValueEnrichable[_]],
      classOf[MultiValueEnrichable[_]],
      classOf[FieldPointer[_, _]],
      classOf[MultiFieldPointer[_, _]],
      classOf[DependentFieldPointer[_, _]],
      classOf[MultiToSingleFieldPointer[_, _]],
      classOf[SingleToMultiFieldPointer[_, _]],
      classOf[NamedFieldPointer[_, _]],
      classOf[PathFieldPointer[_, _]],
      classOf[IdentityField[_]],
      classOf[MultiValueEnrichable[_]],
      classOf[SingleValueEnrichable[_]],
      classOf[Data[_]],
      classOf[Entities],
      classOf[HtmlTag],
      classOf[HtmlTags],
      classOf[HtmlAttribute],
      classOf[Values[_]]
    ))
  }

  def load[Raw : ClassTag, Parsed : ClassTag](spec: DataSpec[Raw, Parsed]): RDD[Parsed] = {
    initialize()
    val sc = SparkContext.getOrCreate
    spec.initialize(sc)
    val raw = spec.load(sc, Sparkling.parallelism)
    val specBc = sc.broadcast(spec)
    Sparkling.initPartitions(raw).mapPartitions{records =>
      val spec = specBc.value
      records.flatMap(spec.parse)
    }
  }
}
