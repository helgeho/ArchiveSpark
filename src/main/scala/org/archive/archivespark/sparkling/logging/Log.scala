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
