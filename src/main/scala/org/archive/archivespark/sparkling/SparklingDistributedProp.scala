package org.archive.archivespark.sparkling

class SparklingDistributedProp (get: => Any, set: Any => Unit) extends Serializable {
  private var value: Any = _
  def save(): Unit = value = get
  def restore(): Unit = set(value)
}
