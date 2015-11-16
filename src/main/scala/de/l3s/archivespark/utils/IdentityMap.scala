/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 */

package de.l3s.archivespark.utils

class IdentityMap[T](elems: (T, T)*) {
  val map = Map[T, T](elems: _*)

  def apply(key: T): T = {
    map.get(key) match {
      case Some(value) => value
      case None => key
    }
  }
}

object IdentityMap {
  def apply[T](elems: (T, T)*) = new IdentityMap[T](elems: _*)
}
