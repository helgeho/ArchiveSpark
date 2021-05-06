package org.archive.archivespark.sparkling.att

case class Attachment[A](checksum: String, content: String, value: Option[A], status: Attachment.Status) {
  import Attachment._
  def isDefined: Boolean = status == Status.Available
  def get: A = value.get
  def toOption: Option[A] = if (isDefined) value else None
  def map[R](f: A => R): Option[R] = toOption.map(f)
  def filter(f: A => Boolean): Option[A] = toOption.filter(f)
}

object Attachment {
  object Status extends Enumeration {
    val Available, NotParsable, Malformed, ChecksumMismatch, Unavailable = Value
  }
  type Status = Status.Value
}