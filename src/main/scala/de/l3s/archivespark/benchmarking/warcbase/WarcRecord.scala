/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.benchmarking.warcbase

import java.text.SimpleDateFormat

import org.archive.util.ArchiveUtils
import org.warcbase.data.WarcRecordUtils
import org.warcbase.io.WarcRecordWritable
import org.warcbase.spark.matchbox.ExtractTopLevelDomain

/*
compare https://github.com/lintool/warcbase/blob/master/src/main/scala/org/warcbase/spark/archive/io/WarcRecord.scala
- make ISO8601 a local variable instead of a field of the class in order to enable serialization using Kryo
- transform as much vals as possible to defs to not compute everything on every record and to not keep everything in memory
- remove trait ArchiveRecord to allow defs instead of vals (trait should never require vals anyway)
 */
class WarcRecord(r: WarcRecordWritable) {
  val date = r.getRecord.getHeader.getDate

  def getCrawldate: String = {
    val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    ArchiveUtils.get14DigitDate(ISO8601.parse(date)).substring(0, 8)
  }

  val getContentBytes: Array[Byte] = WarcRecordUtils.getContent(r.getRecord)

  def getContentString: String = new String(getContentBytes)

  def getMimeType = WarcRecordUtils.getWarcResponseMimeType(getContentBytes)

  val getUrl = r.getRecord.getHeader.getUrl

  def getDomain = ExtractTopLevelDomain(getUrl)
}