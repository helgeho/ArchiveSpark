package de.l3s.archivespark

import de.l3s.archivespark.enrich.{Derivatives, EnrichFunc, Enrichable}
import de.l3s.archivespark.utils.Copyable
import de.l3s.archivespark.utils.Json._
import de.l3s.archivespark.utils.Json._

import scala.reflect.ClassTag

class MultiValueArchiveRecordField[T] (val children: Seq[Enrichable[T, _]]) extends Enrichable[Seq[T], MultiValueArchiveRecordField[T]] {
  def get: Seq[T] = children.map(e => e.get)

  override def enrich(path: Seq[String], func: EnrichFunc[_, _], excludeFromOutput: Boolean): MultiValueArchiveRecordField[T] = {
    if (path.nonEmpty && path.head == "*") {
      var hasEnriched = false
      val enriched = children.map{c =>
        val enriched = c.enrich(path.tail, func, excludeFromOutput).asInstanceOf[Enrichable[T, _]]
        hasEnriched = hasEnriched || enriched != c
        enriched
      }
      if (hasEnriched) {
        val clone = new MultiValueArchiveRecordField(enriched)
        clone._root = _root
        clone._parent = _parent
        clone
      } else this
    } else if (path.nonEmpty && path.head.matches("\\[\\d+\\]")) {
      val index = path.head.substring(1, path.head.length - 1).toInt
      if (index < children.length) {
        val enriched = children.zipWithIndex.map{case (c, i) => if (i == index) c.enrich(path.tail, func, excludeFromOutput).asInstanceOf[Enrichable[T, _]] else c}
        if (children(index) == enriched(index)) this
        else new MultiValueArchiveRecordField(enriched)
      } else this
    } else super.enrich(path, func, excludeFromOutput)
  }

  def apply(index: Int): Enrichable[T, _] = children(index)

  def get(index: Int): T = apply(index).get

  override def apply[D : ClassTag](path: Seq[String]): Option[Enrichable[D, _]] = {
    if (path.nonEmpty && path.head == "*") {
      val values = children.map(c => c(path.tail)).filter(_.isDefined).map(_.get)
      if (values.isEmpty) None else {
        val clone = new MultiValueArchiveRecordField(values)
        clone._root = _root
        clone._parent = _parent
        Some(clone.asInstanceOf[Enrichable[D, MultiValueArchiveRecordField[T]]])
      }
    } else if (path.nonEmpty && path.head.matches("\\[\\d+\\]")) {
      val index = path.head.substring(1, path.head.length - 1).toInt
      if (index < children.length) children(index)(path.tail) else None
    } else super.apply(path)
  }

  def toJson: Map[String, Any] = (if (isExcludedFromOutput) Map() else Map(
    null.asInstanceOf[String] -> children.map(c => mapToJsonValue(c.toJson))
  )) ++ enrichments.map{e => (e, mapToJsonValue(enrichment(e).get.toJson)) }.filter{ case (_, field) => field != null }
}

object MultiValueArchiveRecordField {
  def apply[T](values: Seq[T]): MultiValueArchiveRecordField[T] = new MultiValueArchiveRecordField[T](values.map(v => ArchiveRecordField(v)))
}