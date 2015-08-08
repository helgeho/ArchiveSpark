package de.l3s.archivespark.enrich

import de.l3s.archivespark.{Enrichable, EnrichRoot}

/**
 * Created by holzmann on 05.08.2015.
 */
trait DependentEnrichFunc[Root <: EnrichRoot[_, Root, Target], Source <: Enrichable[_, Source, Target], Target <: Enrichable[_, _, Target]] extends EnrichFunc[Root, Source, Target] {
  def dependency: EnrichFunc[Root, _, Source]

  def dependencyField: String

  def source: Seq[String] = dependency.source :+ dependency.field(dependencyField)

  def on(dependency: EnrichFunc[Root, _, Source]): EnrichFunc[Root, _, _] = new PipedEnrichFunc[Root, Source, Target](this, dependency)

  override def checkExistence(root: Root): Boolean = {
    if(!dependency.checkExistence(root)) { root.enrich(dependency); false }
    else super.checkExistence(root)
  }
}
