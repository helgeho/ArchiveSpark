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

package de.l3s.archivespark.cdx

import de.l3s.archivespark.utils.JsonConvertible

class ResolvedCdxRecord(original: CdxRecord, val location: LocationInfo, val parentRecord: ResolvedCdxRecord) extends JsonConvertible with Serializable {
  def this(original: CdxRecord, locationPath: String, parentRecord: ResolvedCdxRecord = null) = {
    this(original, LocationInfoImpl(original.location.compressedSize, original.location.offset, original.location.filename, locationPath), parentRecord)
  }

  def surtUrl = original.surtUrl
  def timestamp = original.timestamp
  def time = original.time
  def originalUrl = original.originalUrl
  def mime = original.mime
  def status = original.status
  def digest = original.digest
  def redirectUrl = original.redirectUrl
  def meta = original.meta

  def toSimpleString = original.toSimpleString

  def toJson: Map[String, Any] = {
    if (parentRecord != null) {
      original.toJson + ("parent" -> parentRecord.toJson)
    } else {
      original.toJson
    }
  }
}
