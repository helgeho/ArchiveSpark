package org.archive.archivespark

import org.archive.archivespark.sparkling.util.StringUtil

package object  sparkling {
  implicit class IntFileUnitExtensions(value: Int) {
    def kb: Long = value * 1024
    def mb: Long = kb * 1024
    def gb: Long = mb * 1024
    def tb: Long = gb * 1024
    def pb: Long = tb * 1024
  }

  implicit class NumberFormatExtensions[A : Numeric](number: A) {
    def readable: String = StringUtil.formatNumber(number)
    def readable(decimal: Int): String = StringUtil.formatNumber(number, decimal)
  }
}
