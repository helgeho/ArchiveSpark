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

import de.l3s.archivespark.utils.IdentityMap

trait EnrichFunc[Root <: EnrichRoot[_], Source <: Enrichable[_]] extends Serializable {
  def source: Seq[String]
  def fields: Seq[String]

  def enrich(root: Root): Root = enrich(root, excludeFromOutput = false)

  private[enrich] def enrich(root: Root, excludeFromOutput: Boolean): Root = {
    if (exists(root)) return root
    enrichPath(root, source, excludeFromOutput).asInstanceOf[Root]
  }

  private def enrichPath(current: Enrichable[_], path: Seq[String], excludeFromOutput: Boolean): Enrichable[_] = {
    if (path.isEmpty) enrichSource(current.asInstanceOf[Source], excludeFromOutput)
    else {
      val field = path.head
      val enrichedField = enrichPath(current._enrichments(field), path.tail, excludeFromOutput)
      val clone = current.copy()
      clone._enrichments = clone._enrichments.updated(field, enrichedField)
      clone
    }
  }

  private def enrichSource(source: Source, excludeFromOutput: Boolean): Enrichable[_] = {
    val derivatives = new Derivatives[Enrichable[_]](fields)
    derive(source, derivatives)
    derivatives.get.values.foreach(e => e.excludeFromOutput(excludeFromOutput, overwrite = false))

    val clone = source.copy()
    clone._enrichments ++= derivatives.get
    clone
  }

  def derive(source: Source, derivatives: Derivatives[Enrichable[_]]): Unit

  def field: IdentityMap[String] = IdentityMap[String]()

  def exists(root: Root): Boolean = {
    var field: Enrichable[_] = root
    for (key <- source) field.enrichments.get(key) match {
      case Some(nextField) => field = nextField
      case None => return false
    }
    if (!fields.forall(f => field.enrichments.contains(f))) return false
    true
  }
}
