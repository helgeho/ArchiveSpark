/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.archive.archivespark.functions

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{NamedEntityTagAnnotation, SentencesAnnotation, TextAnnotation, TokensAnnotation}
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import org.archive.archivespark.model._
import org.archive.archivespark.model.dataloads.ByteLoad
import org.archive.archivespark.model.pointers.DependentFieldPointer

import scala.collection.JavaConverters._
import scala.collection.mutable

object EntitiesNamespace {
  def get: DependentFieldPointer[ByteLoad.Root, String] = HtmlText.mapIdentity("entities").get[String]("entities")
}

/*
In order to use this enrich function, please make sure have Stanford CoreNLP (3.5.1) with corresponding models in your classpath.
http://central.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.5.1
 */
class Entities private (properties: Properties, tagFieldMapping: Seq[(String, String)]) extends BoundEnrichFunc[ByteLoad.Root, String, String](EntitiesNamespace.get) {
  override def defaultField: String = ""

  override def fields: Seq[String] = tagFieldMapping.map { case (tag, field) => field }

  @transient lazy val pipeline: StanfordCoreNLP = new StanfordCoreNLP(properties)

  override def derive(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    val doc = new Annotation(source.get)
    pipeline.annotate(doc)
    val sentences: mutable.Seq[CoreMap] = doc.get(classOf[SentencesAnnotation]).asScala
    val mentions = sentences.flatMap { sentence =>
      val tokens: mutable.Buffer[CoreLabel] = sentence.get(classOf[TokensAnnotation]).asScala
      tokens.map { token =>
        val word: String = token.get(classOf[TextAnnotation])
        val ne: String = token.get(classOf[NamedEntityTagAnnotation])
        (ne, word)
      }
    }.groupBy{case (ne, word) => ne.toLowerCase}.mapValues(items => items.map{case (ne, word) => word}.toSet)
    for ((tag, _) <- tagFieldMapping) derivatives.setNext(MultiValueEnrichable(mentions.getOrElse(tag.toLowerCase, Set()).toSeq))
  }
}

object EntitiesConstants {
  val DefaultTagFieldMapping: Seq[(String, String)] = Seq(
    "PERSON" -> "persons",
    "ORGANIZATION" -> "organizations",
    "LOCATION" -> "locations",
    "DATE" -> "dates"
  )

  val DefaultProps: Properties = new Properties() {{
    setProperty("annotators", "tokenize, ssplit, pos, lemma, ner")
    setProperty("tokenize.class", "PTBTokenizer")
    setProperty("tokenize.language", "en")
    setProperty("ner.useSUTime", "false")
    setProperty("ner.applyNumericClassifiers", "false")
  }}
}

object Entities extends Entities(EntitiesConstants.DefaultProps, EntitiesConstants.DefaultTagFieldMapping) {
  def apply() = new Entities(EntitiesConstants.DefaultProps, EntitiesConstants.DefaultTagFieldMapping)
  def apply(tagFieldMapping: (String, String)*) = new Entities(EntitiesConstants.DefaultProps, tagFieldMapping)
  def apply(props: Properties) = new Entities(props, EntitiesConstants.DefaultTagFieldMapping)
  def apply(props: Properties, tagFieldMapping: (String, String)*) = new Entities(props, tagFieldMapping)
  def apply(language: String, tagFieldMapping: Seq[(String, String)] = EntitiesConstants.DefaultTagFieldMapping): Entities = {
    val props = EntitiesConstants.DefaultProps
    props.setProperty("tokenize.language", language)
    new Entities(props, tagFieldMapping)
  }
}
