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

package de.l3s.archivespark.enrich.functions

import de.l3s.archivespark.utils.IdentityMap
import de.l3s.archivespark.{ArchiveRecordField, ResolvedArchiveRecord}
import de.l3s.archivespark.enrich.{EnrichFunc, Enrichable, Derivatives, DependentEnrichFunc}
import org.apache.http.HttpEntity
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.util.EntityUtils

object StringContent extends DependentEnrichFunc[ResolvedArchiveRecord, ArchiveRecordField[Array[Byte]]] {
  override def dependency: EnrichFunc[ResolvedArchiveRecord, _] = Response
  override def dependencyField: String = "content"

  override def fields: Seq[String] = Seq("string")
  override def field: IdentityMap[String] = IdentityMap(
    "text" -> "string"
  )

  override def derive(source: ArchiveRecordField[Array[Byte]], derivatives: Derivatives[Enrichable[_]]): Unit = {
    val entity: HttpEntity = new ByteArrayEntity(source.get)
    derivatives << ArchiveRecordField(EntityUtils.toString(entity).trim)
  }
}
