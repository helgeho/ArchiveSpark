package org.archive.archivespark.sparkling.util

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.Try

object Time14Util {
  val TimestampFill: String = "20000101000000"
  val TimestampFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC

  def fix(timestamp: String): String = {
    val fill = TimestampFill
    val length = fill.length
    if (timestamp.length < length) timestamp + fill.takeRight(length - timestamp.length) else timestamp
  }

  def parse(timestamp: String, fix: Boolean = true): DateTime = TimestampFormat.parseDateTime(if (fix) this.fix(timestamp) else timestamp)

  def validate(timestamp: String): Option[String] = {
    val fixed = fix(timestamp)
    if (Try(parse(fixed, fix = false)).isSuccess) Some(fixed)
    else None
  }
}
