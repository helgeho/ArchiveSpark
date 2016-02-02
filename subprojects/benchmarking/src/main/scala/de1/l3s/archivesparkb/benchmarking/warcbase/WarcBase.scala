/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de1.l3s.archivesparkb.benchmarking.warcbase

import java.io.ByteArrayInputStream

import de.l3s.archivespark.utils.HttpArchiveRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.archive.io.ArchiveReaderFactory
import org.warcbase.io.WarcRecordWritable
import org.warcbase.mapreduce.WacWarcInputFormat
import scala.collection.JavaConverters._

object WarcBase {
  def hbase(table: String)(conf: Configuration => Unit)(implicit sc: SparkContext) = {
    HBase.rdd(table) { c =>
      c.set(TableInputFormat.SCAN_COLUMN_FAMILY, "c"); // content family of WarcBase
      c.setInt(TableInputFormat.SCAN_MAXVERSIONS, Int.MaxValue); // get all versions
      conf(c)
    }
  }

  def flatVersions(hbaseRdd: RDD[Result]) = {
    hbaseRdd.flatMap{ result =>
      for (cell <- result.listCells().asScala) yield { // one record per version / capture
        val url = CellUtil.getCellKeyAsString(cell)
        val mime = Bytes.toString(CellUtil.cloneQualifier(cell))
        val valueBytes = CellUtil.cloneValue(cell)
        var value = Bytes.toString(valueBytes)

        val warcPath = "[^\\ ]+\\.[w]?arc(\\.gz)?".r.findFirstIn(value).get
        val warcId = if (warcPath.endsWith(".gz")) warcPath.substring(0, warcPath.length - 3) else warcPath
        val reader = ArchiveReaderFactory.get(warcId, new ByteArrayInputStream(valueBytes), false)
        (cell.getTimestamp, url, mime, HttpArchiveRecord(reader.get))
      }
    }
  }

  /*
  compare https://github.com/lintool/warcbase/blob/master/src/main/scala/org/warcbase/spark/matchbox/RecordLoader.scala#L32
  - only load records with positive content length but less than 100 MB to avoid memory issues
  - removed any other prefilters
  - return custom WarcRecord class instead of ArchiveRecord trait
   */
  def loadWarc(path: String)(implicit sc: SparkContext): RDD[WarcRecord] = {
    sc.newAPIHadoopFile(path, classOf[WacWarcInputFormat], classOf[LongWritable], classOf[WarcRecordWritable])
    .filter{case (_, r) => r.getRecord.getHeader.getContentLength > 0 && r.getRecord.getHeader.getContentLength < 100 * 1024 * 1024}
    .map{case (_, r) => new WarcRecord(r)}
  }
}
