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

package de.l3s.archivespark.enrich

class Derivatives[T](val fields: Seq[String]) {
  private var nextField = 0
  private var map = Map[String, T]()

  def get: Map[String, T] = map

  def <<(value: T) = {
    map += fields(nextField) -> value
    nextField += 1
  }
}
