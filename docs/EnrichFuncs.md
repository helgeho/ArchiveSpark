[< Table of Contents](README.md) | [Contribute >](Contribute.md)
:---|---:

# Enrich Functions

Enrich Functions constitute one of the main elements of ArchiveSpark modular design.
They serve as the bridge between third-party tools and data stored in the ArchiveSpark data model by providing a simple, abstract and declarative interface.
At the same time, they are used as pointers to the location of the corresponding information within an ArchiveSpark record.
Multiple Enrich Functions can be chained to allow flexible pipelines of data derivation and extraction.

These Enrich Functions become available with the following import: `import org.archive.archivespark.enrich.functions._`

EnrichFunc | Description
:-------|:--- 
**[StringContent](../src/main/scala/de/l3s/archivespark/enrich/functions/StringContent.scala)** | Converts bytes to their corresponding string representation. This can directly be used to access the string content of a text record, e.g., the HTML code of a webpage.
&nbsp; | *Example:* `val enriched = rdd.enrich(LowerCase.of(HtmlText.of(Html.first("title")))` 
**[Html](../src/main/scala/de/l3s/archivespark/enrich/functions/Html.scala).first**(*selector*) | Extracts the first occurrence of a tag that matches the selector. The result includes the tags, i.e., `<title>The Title</title>`. To get the actual text only it can be combined with `HtmlText`, e.g., `HtmlText.of(Html.first("title"))`. By default, it depends on the `StringContent` of a record and can directly be applied to text records that comprise HTML code.
&nbsp; | *Example:* `val enriched = rdd.enrich(Html.first("title"))` 
**[Html](../src/main/scala/de/l3s/archivespark/enrich/functions/Html.scala).all**(*selector*) | Extracts all occurrence of tags that match the selector. The result is a multi-value list of matches, which includes the tag as shown for `Html.first(...)` above. To get the texts it can be combined with `HtmlText`, e.g. `HtmlText.ofEach(Html.all("a"))`. By default, it depends on the `StringContent` of a record and can directly be applied to text records that comprise HTML code.  
&nbsp; | *Example:* `val enriched = rdd.enrich(Html.all("a"))` 
**[HtmlText](../src/main/scala/de/l3s/archivespark/enrich/functions/HtmlText.scala)** | Extracts the text from a HTML tag.  By default, it depends on the body tag (`Html.first("body")`) of the record it is applied to.
&nbsp; | *Example:* `val enriched = rdd.enrich(HtmlText.of(Html.first("title"))` 
**[HtmlAttribute](../src/main/scala/de/l3s/archivespark/enrich/functions/HtmlAttribute.scala)**(*name*) | Extracts the value of the attribute with given name from a HTML tag.
&nbsp; | *Example:* `val enriched = rdd.enrich(HtmlAttribute("href").ofEach(Html.all("a"))` 
**[LowerCase](../src/main/scala/de/l3s/archivespark/enrich/functions/LowerCase.scala)** | Turns a string value to lower case.
&nbsp; | *Example:* `val enriched = rdd.enrich(LowerCase.of(HtmlText.of(Html.first("title")))` 
**[SURT](../src/main/scala/de/l3s/archivespark/enrich/functions/SURT.scala)** | Converts an URL to its canonicalized [SURT form](http://crawler.archive.org/articles/user_manual/glossary.html#surt) without the protocol, e.g., `http://archive.org/web` -> `org,archive)/web`. For relative URLs extracted from a record, it automatically attempts to resolve the absolute form before conversion.
&nbsp; | *Example:* `val enriched = rdd.enrich(SURT.of(HtmlAttribute("href").ofEach(Html.all("a")))` 
**[Entities](../src/main/scala/de/l3s/archivespark/enrich/functions/Entities.scala)** | Extracts *persons*, *organizations*, *locations* and *dates* from text. By default, it depends on the `HtmlText` extracted from a webpage. In order to use this Enrich Function you need to add [`edu.stanford.nlp:stanford-corenlp:3.4.1`](http://central.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.4.1/) with corresponding models to your classpath. See at the bottom of this article for another Enrich Function for more accurate Entity Linking ([FEL4ArchiveSpark](https://github.com/helgeho/FEL4ArchiveSpark)).
&nbsp; | *Example:* `val enriched = rdd.enrich(Entities.of(Html.first("title")))`
**[Root](../src/main/scala/de/l3s/archivespark/enrich/functions/Root.scala)**[*M*] | Provides access to the root value of a record. This is commonly the meta data, e.g., the `CdxRecord` in case of a Web archive record. The type of this data (*M*) needs to be specified.
&nbsp; | *Example:* `val enriched = rdd.mapValues(Root[CdxRecord].map("year")(_.timestamp.take(4)))`  
**[Values](../src/main/scala/de/l3s/archivespark/enrich/functions/Values.scala)**(*field*, *funcs**) | Combines the values of multiple Enrich Functions to a single array. The field specified the name of the resulting value. This is helpful to map a dataset to these values.
&nbsp; | *Example:* `val Links = Html.all("a")`<br>`val LinkTexts = HtmlText.ofEach(Links)`<br>`val LinkUrls = HtmlAttribute("href").ofEach(Links)`<br>`val enriched = rdd.enrich(LinkTexts).enrich(LinkUrls)`<br>`val links = enriched.flatMapValues(Values("links", LinkTexts, LinkUrls))`

## Web Archive-specific Enrich Functions

The following Web Archive-specific should only be used explicitely by advanced users. They rather serve as default accessors to the content of a Web archive record, which are implicitely used by other Enrich Function. They become available through `import org.archive.archivespark.specific.warc.enrichfunctions._`

EnrichFunc | Description
:-------|:--- 
**[WarcPayload](../src/main/scala/de/l3s/archivespark/specific/warc/enrichfunctions/WarcPayload.scala)** | Enriches the meta data of a (W)ARC record with the WARC headers, HTTP headers as well as the raw payload as bytes. This is the default Enrich Function to get access to a Web archive record, i.e., all Enrich Functions that depend on byte content, such as `StringContent` automatically depend on this Enrich Function by default.
&nbsp; | *Example:* `val enriched = rdd.enrich(WarcPayload)` 
**[HttpPayload](../src/main/scala/de/l3s/archivespark/specific/warc/enrichfunctions/HttpPayload.scala)** | The same as `WarcPayload` but for plain HTTP records without WARC headers, e.g., when loaded remotely from the Wayback Machine. 
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WaybackSpec("http://l3s.de")).enrich(HttpPayload)`

## More Enrich Functions

Additional Enrich Functions may be found in external projects, which is the recommended way to [share your Enrich Functions](Dev_EnrichFuncs.md), such as:

* An Enrich Function to use [Yahoo's Fast Entity Linker](https://github.com/yahoo/FEL) (FEL) with ArchiveSpark: [FEL4ArchiveSpark](https://github.com/helgeho/FEL4ArchiveSpark).

[< Table of Contents](README.md) | [Contribute >](Contribute.md)
:---|---: 