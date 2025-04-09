/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2024 Helge Holzmann (Internet Archive) <helge@archive.org>
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

package org.archive.webservices.archivespark.functions

import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.archive.webservices.archivespark.model._
import org.archive.webservices.archivespark.model.dataloads.ByteLoad
import org.archive.webservices.sparkling.ars.WANE
import org.archive.webservices.sparkling.util.RegexUtil

import java.util.Properties

object EntitiesNamespace {
  def get: EnrichFunc[ByteLoad.Root, EnrichRoot, String, String] = HtmlText.mapIdentity("entities")
}

/*
In order to use this enrich function, please make sure you have Stanford CoreNLP (3.5.1) with corresponding models in your classpath.
http://central.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.5.1
 */
class Entities (
                 properties: Properties = EntitiesConstants.DefaultProps,
                 tagFieldMapping: Seq[(String, String)] = EntitiesConstants.DefaultTagFieldMapping,
                 cleanLatin: Boolean = false) extends BoundMultiEnrichFunc[ByteLoad.Root, EnrichRoot, String, String, String](EntitiesNamespace.get) {
  override def defaultField: String = ""

  override def fields: Seq[String] = tagFieldMapping.map { case (tag, field) => field }

  @transient lazy val pipeline: StanfordCoreNLP = new StanfordCoreNLP(properties)

  override def deriveBound(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    val text = if (cleanLatin) RegexUtil.cleanLatin(source.get) else source.get
    val mentions = WANE.entities(text, pipeline)
    for ((tag, _) <- tagFieldMapping) {
      derivatives.setNext(MultiValueEnrichable(mentions.getOrElse(tag, Set.empty).toSeq))
    }
  }
}

object EntitiesConstants {
  val DefaultTagFieldMapping: Seq[(String, String)] = Seq(
    "PERSON" -> "persons",
    "ORGANIZATION" -> "organizations",
    "LOCATION" -> "locations",
    "DATE" -> "dates"
  )

  val DefaultProps: Properties = WANE.properties
}

object Entities extends Entities(EntitiesConstants.DefaultProps, EntitiesConstants.DefaultTagFieldMapping, cleanLatin = true) {
  def apply(tagFieldMapping: (String, String)*) = new Entities(EntitiesConstants.DefaultProps, tagFieldMapping)
  def apply(props: Properties) = new Entities(props, EntitiesConstants.DefaultTagFieldMapping)
  def apply(props: Properties, tagFieldMapping: (String, String)*) = new Entities(props, tagFieldMapping)
  def apply(language: String, tagFieldMapping: Seq[(String, String)] = EntitiesConstants.DefaultTagFieldMapping): Entities = {
    val props = EntitiesConstants.DefaultProps
    props.setProperty("tokenize.language", language)
    new Entities(props, tagFieldMapping)
  }

  def onLatin(props: Properties = EntitiesConstants.DefaultProps, tagFieldMapping: Seq[(String, String)] = EntitiesConstants.DefaultTagFieldMapping): Entities = {
    new Entities(props, EntitiesConstants.DefaultTagFieldMapping, cleanLatin = true)
  }
}
