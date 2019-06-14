/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2018 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package org.archive.archivespark.sparkling.util

import java.io.InputStream

import org.apache.commons.codec.binary.Base32
import org.apache.commons.codec.digest.DigestUtils

object DigestUtil {
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
