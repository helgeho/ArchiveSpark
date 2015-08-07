package de.l3s.archivespark

import de.l3s.archivespark.enrich.EnrichFunc

/**
 * Created by holzmann on 05.08.2015.
 */
trait EnrichRoot[Root <: EnrichRoot[Root]] {
  def enrich(f: EnrichFunc[Root]): Root = {
    val entry = f.entry(this.asInstanceOf[Root])
    val field = entry.enrichable

    // apply enrich func
    entry.switch(field)
  }
}
