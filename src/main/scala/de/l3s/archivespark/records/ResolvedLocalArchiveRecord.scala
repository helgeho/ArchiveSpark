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

package de.l3s.archivespark.records

import java.io.{FileInputStream, InputStream}
import java.nio.file.Paths

import de.l3s.archivespark.ResolvedArchiveRecord
import de.l3s.archivespark.cdx.ResolvedCdxRecord
import org.apache.commons.io.input.BoundedInputStream

import scala.util.Try

class ResolvedLocalArchiveRecord(cdx: ResolvedCdxRecord) extends ResolvedArchiveRecord(cdx) {
  override def access[R >: Null](action: (String, InputStream) => R): R = {
    if (cdx.location.compressedSize < 0 || cdx.location.offset < 0) null
    else {
      var stream: FileInputStream = null
      try {
        stream = new FileInputStream(Paths.get(cdx.location.fileLocation, cdx.location.filename).toString)
        stream.skip(cdx.location.offset)
        action(cdx.location.filename, new BoundedInputStream(stream, cdx.location.compressedSize))
      } catch {
        case e: Exception => null /* something went wrong, do nothing */
      } finally {
        if (stream != null) Try {stream.close()}
      }
    }
  }
}
