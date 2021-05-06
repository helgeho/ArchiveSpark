package org.archive.archivespark.sparkling.util

import java.io.InputStream

import org.apache.commons.codec.binary.Base32
import org.apache.commons.codec.digest.DigestUtils

object DigestUtil {
  def md5Hex(in: InputStream): String = DigestUtils.md5Hex(in)
  def md5Hex(bytes: Array[Byte]): String = DigestUtils.md5Hex(bytes)
  def md5Hex(str: String): String = DigestUtils.md5Hex(str)

  def sha1Hex(in: InputStream): String = DigestUtils.sha1Hex(in)
  def sha1Hex(bytes: Array[Byte]): String = DigestUtils.sha1Hex(bytes)
  def sha1Hex(str: String): String = DigestUtils.sha1Hex(str)

  def sha1Base32(in: InputStream): String = {
    val digest = DigestUtils.sha1(in)
    new Base32().encodeAsString(digest).toUpperCase
  }

  def sha1Base32(bytes: Array[Byte]): String = {
    val digest = DigestUtils.sha1(bytes)
    new Base32().encodeAsString(digest).toUpperCase
  }

  def sha1Base32(str: String): String = {
    val digest = DigestUtils.sha1(str)
    new Base32().encodeAsString(digest).toUpperCase
  }
}
