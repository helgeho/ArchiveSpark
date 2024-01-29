[< Table of Contents](README.md) | [Contribute >](Contribute.md)
:---|---:

# Enrichment Functions

Enrichment Functions constitute one of the main elements of ArchiveSpark modular design.
They serve as the bridge between third-party tools and data stored in the ArchiveSpark data model by providing a simple, abstract and declarative interface.
At the same time, they are used as pointers to the location of the corresponding information within an ArchiveSpark record.
Multiple Enrichment Functions can be chained to allow flexible pipelines of data derivation and extraction.

These Enrichment Functions become available with the following import: `import org.archive.webservices.archivespark.functions._`

EnrichFunc | Description
:-------|:--- 
**[StringContent](../src/main/scala/org/archive/archivespark/functions/StringContent.scala)** | Converts bytes to their corresponding string representation, e.g., the HTML code of a webpage.
&nbsp; | *Example:* `val enriched = rdd.enrich(LowerCase.of(HtmlText.of(Html.first("title")))` 
**[Html](../src/main/scala/org/archive/archivespark/functions/Html.scala).first**(*selector*) | Extracts the first occurrence of a tag that matches the specified tag name. The result includes the full tags, i.e., `<title>The Title</title>`. To get the actual text only it can be combined with `HtmlText`, e.g., `HtmlText.of(Html.first("title"))`. By default, it depends on the `StringContent` of a record and can directly be applied to text records that are comprised of HTML code.
&nbsp; | *Example:* `val enriched = rdd.enrich(Html.first("title"))` 
**[Html](../src/main/scala/org/archive/archivespark/functions/Html.scala).all**(*selector*) | Extracts all occurrence of tags that match the specified tag name. The result is a multi-value list of matches, which include the full tags as shown for `Html.first(...)` above. To get the texts out, it can be combined with `HtmlText`, e.g. `HtmlText.ofEach(Html.all("a"))`. By default, it depends on the `StringContent` of a record and can directly be applied to text records that comprise HTML code.  
&nbsp; | *Example:* `val enriched = rdd.enrich(Html.all("a"))` 
**[HtmlText](../src/main/scala/org/archive/archivespark/functions/HtmlText.scala)** | Extracts the text from an HTML tag. By default, it depends on `Html.first("body")`, which represents the full body of a page, i.e., the visible text.
&nbsp; | *Example:* `val enriched = rdd.enrich(HtmlText.of(Html.first("title"))` 
**[HtmlAttribute](../src/main/scala/org/archive/archivespark/functions/HtmlAttribute.scala)**(*name*) | Extracts the value of the attribute with given name from an HTML tag.
&nbsp; | *Example:* `val enriched = rdd.enrich(HtmlAttribute("href").ofEach(Html.all("a"))` 
**[LowerCase](../src/main/scala/org/archive/archivespark/functions/LowerCase.scala)** | Turns a string value to lower case.
&nbsp; | *Example:* `val enriched = rdd.enrich(LowerCase.of(HtmlText.of(Html.first("title")))` 
**[SURT](../src/main/scala/org/archive/archivespark/functions/SURT.scala)** | Converts an URL to its canonicalized [SURT form](http://crawler.archive.org/articles/user_manual/glossary.html#surt) without the protocol, e.g., `http://archive.org/web` -> `org,archive)/web`. For relative URLs extracted from a record, it automatically attempts to resolve the absolute URL before conversion.
&nbsp; | *Example:* `val enriched = rdd.enrich(SURT.of(HtmlAttribute("href").ofEach(Html.all("a")))` 
**[Entities](../src/main/scala/org/archive/archivespark/functions/Entities.scala)** | Extracts *persons*, *organizations*, *locations* and *dates* from text. By default, it depends on the `HtmlText` extracted from a webpage. In order to use this Enrichment Function you need to add [`edu.stanford.nlp:stanford-corenlp:3.5.1`](http://central.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.5.1/) with corresponding models to your classpath.
&nbsp; | *Example:* `val enriched = rdd.enrich(Entities.of(Html.first("title")))`
**[Values](../src/main/scala/org/archive/archivespark/functions/Values.scala)**(*field*, *funcs**) | Combines the values of multiple Enrichment Functions to a single array. The field specifies the name of the resulting value. This is helpful to `map` a dataset to these values.
&nbsp; | *Example:* `val Links = Html.all("a")`<br>`val LinkTexts = HtmlText.ofEach(Links)`<br>`val LinkUrls = HtmlAttribute("href").ofEach(Links)`<br>`val enriched = rdd.enrich(LinkTexts).enrich(LinkUrls)`<br>`val links = enriched.flatMapValues(Values("links", LinkTexts, LinkUrls))`

## Web Archive-specific Enrichment Functions

The following web archive-specific Enrichment Functions should only be used explicitely by advanced users. They usually serve as default accessors for the content of a web archive record, which are implicitely used by other Enrichment Function. They become available through `import org.archive.webservices.archivespark.specific.warc.functions._`

EnrichFunc | Description
:-------|:--- 
**[WarcPayload](../src/main/scala/org/archive/archivespark/specific/warc/enrichfunctions/WarcPayload.scala)** | Enriches the meta data of a (W)ARC record with the WARC headers, HTTP headers as well as the raw payload as bytes. This is the default Enrichment Function to get access to a web archive record, i.e., all Enrichment Functions that depend on byte content, such as `StringContent` automatically depend on this Enrichment Function by default.
&nbsp; | *Example:* `val enriched = rdd.enrich(WarcPayload)` 
**[HttpPayload](../src/main/scala/org/archive/archivespark/specific/warc/enrichfunctions/HttpPayload.scala)** | The same as `WarcPayload` but for plain HTTP records without WARC headers, e.g., when loaded remotely from the Wayback Machine. 
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromWayback("http://helgeholzmann.de")).enrich(HttpPayload)`

## More Enrichment Functions

Additional Enrichment Functions may be found in external projects, which is the recommended way to [share your Enrichment Functions](Dev_EnrichFuncs.md), such as:

* An Enrichment Function to use [Yahoo's Fast Entity Linker](https://github.com/yahoo/FEL) (FEL) with ArchiveSpark: [FEL4ArchiveSpark](https://github.com/helgeho/FEL4ArchiveSpark).

[< Table of Contents](README.md) | [Contribute >](Contribute.md)
:---|---: 