package de.l3s.archivespark.enrich

import de.l3s.archivespark.{Enrichable, EnrichRoot}

/**
 * Created by holzmann on 05.08.2015.
 */
trait DependentEnrichFunc[Root <: EnrichRoot[_, Root], Source <: Enrichable[_, Source]] extends EnrichFunc[Root, Source] {
  def dependency: EnrichFunc[Root, _]

  def dependencyField: String

  def source: Seq[String] = dependency.source :+ dependency.field(dependencyField)

  def on(dependency: EnrichFunc[Root, _]): DependentEnrichFunc[Root, Source] = new PipedEnrichFunc[Root, Source](this, dependency)

  override def exists(root: Root): Boolean = {
    if (!dependency.exists(root)) { root.enrich(dependency); false }
    else super.exists(root)
  }
}
