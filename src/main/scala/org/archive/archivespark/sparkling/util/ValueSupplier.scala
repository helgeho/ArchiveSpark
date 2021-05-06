package org.archive.archivespark.sparkling.util

class ValueSupplier[A] private (v: => A) {
  def get: A = v
}

object ValueSupplier {
  def apply[A](v: => A): ValueSupplier[A] = new ValueSupplier(v)
}