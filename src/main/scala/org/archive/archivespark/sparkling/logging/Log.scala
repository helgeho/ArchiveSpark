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

package org.archive.archivespark.sparkling.logging

import org.archive.archivespark.sparkling.Sparkling
import org.joda.time.Instant

object Log {
  var level: Int = Sparkling.prop(LogLevels.Info)(level, level = _)
  var logLocal: Boolean = Sparkling.prop(false)(logLocal, logLocal = _)

  val MsgSeparator = " -- "

  def prependLogInfo(msg: String)(implicit context: LogContext): String = prependInfo(msg, Sparkling.loggingId, context.id)

  def prependInfo(msg: String, info: String*): String = "[" + Instant.now().toString() + " - " + info.mkString(" - ") + "] " + msg

  def prependTimestamp(msg: String): String = "[" + Instant.now().toString() + "] " + msg

  def errln(msg: String, prependTimestamp: Boolean = true): Unit = {
    System.err.println(if (prependTimestamp) this.prependTimestamp(msg) else msg)
  }

  def println(msg: String, prependTimestamp: Boolean = true): Unit = {
    System.out.println(if (prependTimestamp) this.prependTimestamp(msg) else msg)
  }

  def log(msg: String, level: Int = LogLevels.Always, log: String => Unit = println(_, prependTimestamp = false))(implicit context: LogContext): Unit = {
    if (level <= this.level && (logLocal || !Sparkling.isLocal)) log(prependLogInfo(msg))
  }

  def info(msg: String*)(implicit context: LogContext): Unit = log(msg.mkString(MsgSeparator), LogLevels.Info)

  def error(msg: String*)(implicit context: LogContext): Unit = log(msg.mkString(MsgSeparator), LogLevels.Error, { msg =>
    println(msg, prependTimestamp = false)
    if (!Sparkling.isLocal) errln(msg, prependTimestamp = false)
  })

  def debug(msg: String*)(implicit context: LogContext): Unit = log(msg.mkString(MsgSeparator), LogLevels.Debug)
}
