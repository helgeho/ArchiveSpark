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

import java.io.InputStream

import de.l3s.archivespark.cdx.ResolvedCdxRecord
import de.l3s.archivespark.enrich.EnrichRoot
import de.l3s.archivespark.utils.Json._

abstract class ResolvedArchiveRecord(override val get: ResolvedCdxRecord) extends EnrichRoot[ResolvedCdxRecord] {
  def access[R >: Null](action: (String, InputStream) => R): R

  override def toJson: Map[String, Any] = (if (isExcludedFromOutput) Map[String, Any]() else Map(
    "record" -> json(this.get)
  )) ++ enrichments.map{ case (name, field) => (name, mapToAny(field.toJson)) }.filter{ case (_, field) => field != null }

  def copy(): ResolvedArchiveRecord = clone().asInstanceOf[ResolvedArchiveRecord]
}
