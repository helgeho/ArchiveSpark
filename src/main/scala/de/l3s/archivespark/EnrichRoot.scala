package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichFunc

/**
 * Created by holzmann on 07.08.2015.
 */
trait EnrichRoot[T, This <: Enrichable[_, _, Children], Children <: Enrichable[_, _, Children]] extends Enrichable[T, This, Children] {
  def enrich(f: EnrichFunc[This, _, _]): This = {
    val root = this.asInstanceOf[This]
    if (!f.checkExistence(root)) enrich(f, f.source)
    else root
  }
}
