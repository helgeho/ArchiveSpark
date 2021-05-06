package org.archive.archivespark.sparkling.html

import org.archive.archivespark.sparkling.html.HtmlProcessor.TagMatch

trait TagHandler[R] {
  def handles(tag: String): Boolean
  def handle(tag: TagMatch, children: Seq[TagMatch]): Unit
  def result: R
  def hierarchical: Boolean
  def handleClosing: Boolean
}

private class NoopTagHandler (val tags: Set[String]) extends TagHandler[Boolean] {
  override def handles(tag: String): Boolean = tags.contains(tag)
  override def handle(tag: TagMatch, children: Seq[TagMatch]): Unit = {}
  override val result: Boolean = true
  override val hierarchical: Boolean = true
  override val handleClosing: Boolean = false
}

private class AllTagHandler[R] (val hierarchical: Boolean, val handleClosing: Boolean, initial: R, handler: (TagMatch, Seq[TagMatch], R) => R) extends TagHandler[R] {
  var currentResult: R = initial
  override def handles(tag: String): Boolean = true
  override def handle(tag: TagMatch, children: Seq[TagMatch]): Unit = currentResult = handler(tag, children, currentResult)
  override def result: R = currentResult
}

private class SpecificTagHandler[R](val tags: Set[String], hierarchical: Boolean, handleClosing: Boolean, initial: R, handler: (TagMatch, Seq[TagMatch], R) => R) extends AllTagHandler[R](hierarchical, handleClosing, initial, handler) {
  override def handles(tag: String): Boolean = tags.contains(tag)
}

object TagHandler {
  def noop(tags: Set[String]): TagHandler[_] = new NoopTagHandler(tags.map(_.toLowerCase))
  def all[R](initial: R, hierarchical: Boolean = false, handleClosing: Boolean = false)(handler: (TagMatch, Seq[TagMatch], R) => R): TagHandler[R] = new AllTagHandler(hierarchical, handleClosing, initial, handler)
  def apply[R](tags: Set[String], initial: R, hierarchical: Boolean = true, handleClosing: Boolean = false)(handler: (TagMatch, Seq[TagMatch], R) => R): TagHandler[R] = new SpecificTagHandler(tags.map(_.toLowerCase), hierarchical, handleClosing, initial, handler)
}