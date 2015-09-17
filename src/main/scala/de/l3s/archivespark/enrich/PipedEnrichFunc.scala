package de.l3s.archivespark.enrich

import de.l3s.archivespark.{EnrichRoot, Enrichable}

/**
 * Created by holzmann on 07.08.2015.
 */
class PipedEnrichFunc[Root <: EnrichRoot[_, Root], Source <: Enrichable[_, Source]]
(parent: DependentEnrichFunc[Root, Source], override val dependency: EnrichFunc[Root, _]) extends DependentEnrichFunc[Root, Source] {
  override def dependencyField: String = parent.dependencyField

  override def derive(source: Source, derivatives: Derivatives[Enrichable[_, _]]): Unit = parent.derive(source, derivatives)

  override def fields: Seq[String] = parent.fields
}
