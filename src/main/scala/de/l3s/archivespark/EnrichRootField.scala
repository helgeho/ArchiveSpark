package de.l3s.archivespark

/**
 * Created by holzmann on 05.08.2015.
 */
class EnrichRootField[T, Root <: EnrichRoot[Root]]
(root: Root, val enrichable: Enrichable[T, _], switchAction: (Root, EnrichRootField[T, Root]) => Unit) {

  def value = enrichable.value

  def switch(enrichable: Enrichable[T, _]): Root = {
    val clone = root.clone().asInstanceOf[Root]
    val newField = new EnrichRootField(clone, enrichable, switchAction)
    switchAction(clone, newField)
    clone
  }

}
