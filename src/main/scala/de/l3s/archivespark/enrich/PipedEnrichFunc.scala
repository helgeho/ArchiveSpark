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

class PipedEnrichFunc[Root <: EnrichRoot[_], Source <: Enrichable[_]]
(parent: DependentEnrichFunc[Root, Source], override val dependency: EnrichFunc[Root, _]) extends DependentEnrichFunc[Root, Source] {
  override def dependencyField: String = parent.dependencyField

  override def derive(source: Source, derivatives: Derivatives[Enrichable[_]]): Unit = parent.derive(source, derivatives)

  override def fields: Seq[String] = parent.fields
}
