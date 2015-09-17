package de.l3s.archivespark.enrich

/**
 * Created by holzmann on 07.08.2015.
 */
trait EnrichRoot[T, This <: EnrichRoot[T, This]] extends Enrichable[T, This]
