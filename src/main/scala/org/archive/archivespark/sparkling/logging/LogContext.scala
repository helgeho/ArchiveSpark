package org.archive.archivespark.sparkling.logging

class LogContext (val id: String) extends Serializable

object LogContext {
  def apply(subject: Any): LogContext = new LogContext(subject.getClass.getSimpleName.stripSuffix("$"))
}