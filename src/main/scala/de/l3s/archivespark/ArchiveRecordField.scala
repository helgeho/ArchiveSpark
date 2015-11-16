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

package de.l3s.archivespark

import de.l3s.archivespark.enrich.Enrichable
import de.l3s.archivespark.utils.Json._

class ArchiveRecordField[T] private (val get: T) extends Enrichable[T] {
  override def toJson: Map[String, Any] = (if (isExcludedFromOutput) Map[String, Any]() else Map(
      null.asInstanceOf[String] -> json(this.get)
    )) ++ enrichments.map{ case (name, field) => (name, mapToAny(field.toJson)) }.filter{ case (_, field) => field != null }

  override def copy(): ArchiveRecordField[T] = clone().asInstanceOf[ArchiveRecordField[T]]
}

object ArchiveRecordField {
  def apply[T](value: T): ArchiveRecordField[T] = new ArchiveRecordField[T](value)
}