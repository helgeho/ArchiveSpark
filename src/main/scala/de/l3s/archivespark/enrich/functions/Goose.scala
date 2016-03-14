/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
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

package de.l3s.archivespark.enrich.functions

import com.gravity.goose.Configuration
import de.l3s.archivespark.cdx.ResolvedCdxRecord
import de.l3s.archivespark.{MultiValueArchiveRecordField, ArchiveRecordField, ResolvedArchiveRecord}
import de.l3s.archivespark.enrich._

private object GooseNamespace extends IdentityEnrichFunction(StringContent, "goose")

class Goose private (val config: Configuration) extends BoundEnrichFunc[ResolvedArchiveRecord, Enrichable[String, _]](GooseNamespace) {
  override def fields = Seq("title", "date", "text", "image", "metaDesc", "metaTags", "videos")

  override def derive(source: Enrichable[String, _], derivatives: Derivatives): Unit = {
    val url = source.root[ResolvedCdxRecord].get.originalUrl
    val goose = new com.gravity.goose.Goose(config)
    val article = goose.extractContent(url, source.get)

    derivatives << ArchiveRecordField(article.title)
    derivatives << ArchiveRecordField(Option(article.publishDate).map(_.toString).getOrElse(""))
    derivatives << ArchiveRecordField(article.cleanedArticleText)
    derivatives << ArchiveRecordField(article.topImage.imageSrc)
    derivatives << ArchiveRecordField(article.metaDescription)
    derivatives << ArchiveRecordField(article.metaKeywords)
    derivatives << MultiValueArchiveRecordField(article.movies.map(e => e.toString))
  }
}

object Goose extends Goose({
    val config = new Configuration
    config.enableImageFetching = false
    config
  }) {
  def newConfig = {
    val config = new Configuration
    config.enableImageFetching = false
    config
  }
  def apply(config: Configuration) = new Goose(config)
}