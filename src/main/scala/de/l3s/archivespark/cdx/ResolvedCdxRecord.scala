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
 */

package de.l3s.archivespark.cdx

import de.l3s.archivespark.utils.JsonConvertible

class ResolvedCdxRecord(original: CdxRecord, locationPath: String, val parentRecord: CdxRecord) extends JsonConvertible {
  def surtUrl = original.surtUrl
  def timestamp = original.timestamp
  def originalUrl = original.originalUrl
  def mime = original.mime
  def status = original.status
  def digest = original.digest
  def redirectUrl = original.redirectUrl
  def meta = original.meta
  val location = LocationInfo(original.location.compressedSize, original.location.offset, original.location.filename, locationPath)

  def toJson: Map[String, Any] = {
    if (parentRecord != null) {
      original.toJson + ("parent" -> parentRecord.toJson)
    } else {
      original.toJson
    }
  }
}
