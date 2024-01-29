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

package org.archive.webservices.archivespark.specific.warc.specs

import org.apache.hadoop.fs.Path
import org.archive.webservices.archivespark.dataspecs.DataSpec
import org.archive.webservices.archivespark.dataspecs.access.{HdfsLocationInfo, HdfsStreamAccessor}
import org.archive.webservices.sparkling.cdx.CdxRecord
import org.archive.webservices.archivespark.specific.warc.WarcRecord
import org.archive.webservices.archivespark.util.FilePathMap

import scala.util.Try

trait WarcHdfsCdxSpecBase[Raw] extends DataSpec[Raw, WarcRecord] {
  protected def filePathMap(warcPath: String) = FilePathMap(warcPath, Seq("(?i).*\\.arc\\.gz", "(?i).*\\.warc\\.gz", "(?i).*\\.wat\\.gz"))

  protected def parse(cdx: CdxRecord, path: Path): Option[WarcRecord] = {
    Try {
      val offset = cdx.locationFromAdditionalFields._2
      new WarcRecord(cdx, new HdfsStreamAccessor(HdfsLocationInfo(path.toString, offset, cdx.compressedSize)))
    }.toOption
  }
}