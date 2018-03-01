[< Table of Contents](README.md) | [Data Specifications (DataSpecs) >](DataSpecs.md)
:---|---:

# ArchiveSpark Operations

ArchiveSpark extends the existing Spark [*transformations*](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations) and [*actions*](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions) with additional operations on a dataset (RDDs), single records (an item in an RDD), as well as [Enrich Functions](EnrichFuncs.md):

## Dataset Operations

Dataset operations that require a *source* (e.g., `mapValues`) or a *field* (e.g., `filterExists`) can commonly be called with either a path (in dot notation, e.g., `rdd.mapValues("payload.string.html.title")`), an Enrich Function with default field (e.g., `rdd.mapValues(HtmlText)`) or an Enrich Function with specified field (e.g., `rdd.mapValues(WarcPayload, "recordHeader")`). In case an Enrich Function with a single or default result field is used as a field pointer, the value of that field can often be inferred automatically, otherwise it needs to be specified, e.g., `rdd.filterValue(StringContent)(v => v.get.length > 10)` vs. `rdd.filterValue("payload.string") {v: Option[String] => v.get.length > 10)`. 

These operations become available by the following import `import de.l3s.archivespark.implicits._`

Operation| Description
:--------|:---
**enrich**(*func*) | Enriches all records in your dataset with the given Enrich Function and returns a new RDD with the enriched records. All dependencies of the given Enrich Function are applied first in case they do not exist yet. The resulting value will be appended in the internal tree-like structure of the records according to its dependency lineage, i.e., `StringContent.of(WarcPayload)` would result in a field called `string` which is nested under another field named `payload`, to be addressed in dot notation as `payload.string`. 
&nbsp; | *Example:* `val enriched = rdd.enrich(StringContent)`
**mapEnrich**(*source*, *target*)(*func*) | Enriches all records in your dataset with the given map function, so you can write the enrichment logic inline using a lambda expression and it will store the result in the *target* field under the *source*. This is equivalent to `rdd.enrich(SourceEnrichFunc.map(target)(func))`
&nbsp; | *Example:* `val enriched = rdd.mapEnrich(StringContent, "length")(_.length)`
**filterExists**(*field*) | Filters the records in the dataset based on whether the given field exists. If the field is specified by an Enrich Function, it checks whether the Enrich Function has returned a result or has resulted in an enrichment.
&nbsp; | *Example:* `val filtered = enriched.filterExists(SomeEnrichFunc)`
**filterValue**(*field*)(*filter*) | Filters the records in the dataset based on the value of the given field. The filter function gets the field value as an option, which is undefined if the field does not exist.
&nbsp; | *Example:* `val filtered = enriched.filterValue(SomeEnrichFunc)(opt => opt.isDefined && opt.get == someValue)`
**filterNonEmpty**(*field*) | Filters the records in the dataset based on whether their value is non-empty, i.e., whether or not the field exists and the value in this field has a non-empty value. This can be applied to any datatype that defines a `nonEmpty` method, e.g., a string can be non-empty if it is longer than 0 characters or an `Option` would be non-empty if it holds a value.
&nbsp; | *Example:* `val filtered = enriched.filterNonEmpty(SomeEnrichFunc)`
**filterNoException**() | Filters the records in the dataset based on whether or not there was an exception thrown during the last enrichment by the applied Enrich Function. Every call of `enrich` clears this last catched exception for each record. To use this operation, [`catchExceptions`](Config.md) has to be enabled (default).
&nbsp; | *Example:* `val filtered = enriched.filterNoException()`
**lastException** | Returns the exception thrown during the last enrichment by the applied Enrich Function. To use this operation, [`catchExceptions`](Config.md) has to be enabled (default). It returns an `Option`, which is only defined if any exception happend. It returns the first exception found in the dataset. Each call of `enrich` clears this last catched exception for each record.
&nbsp; | *Example:* `val ex = enriched.lastException`
**distinctValue**(*field*)(*reduce*) | Returns a new RDD that consists only of records with distinct values in the given field. The given reduce function defined which of two records two keep in case they share the same value in the specified field. 
&nbsp; | *Example:* `val distinct = enriched.distinctValue(_.surtUrl) {(r1, r2) => Seq(r1, r2).maxBy(_.time)}`
**mapValues**(*source*) | Transforms the records in the dataset to the value in the given source field. If you specify the source by an Enrich Function it will automatically do the enrichment before the mapping in case it does not exist yet.
&nbsp; | *Example:* `val titles = rdd.mapValues(Html.first("title"))`
**flatMapValues**(*source*) | Transforms the records in the datset to the value in the given source field and flattens it, i.e., if the source field holds a list, the items of this list are becoming the records of the resulting dataset. If you specify the source by an Enrich Function it will automatically do the enrichment before the mapping in case it does not exist yet.
&nbsp; | *Example:* `val links = rdd.flatMapValues(Html.all("a"))`
**peekJson** | Returns the first record of the dataset as JSON. The tree-like structure of an ArchiveSpark record is reflected by nested JSON objects in this representation. It is very useful to get an idea of what fields the records in your dataset consist of. If you use Jupyter to instruct ArchiveSpark interactively, you need to wrap it into a `print` in order to see the full output, in case the content of the record is too long.
&nbsp; | *Example:* `print(enriched.peekJson)`
**saveAsJson**(*path*) | Saves the records of the dataset as their corresponding JSON representation. This is the same as you get by calling `peekJson` on an RDD. If the path ends in a `.gz` extension, the resulting output will automatically be compressed using GZip.
&nbsp; | *Example:* `enriched.saveAsJson("enriched.json.gz")`

### Web Archive-specific Dataset Operations

These operations are specific to Web archive dataset and become available through `import de.l3s.archivespark.specific.warc._`

Operation| Description
:--------|:---
**resolveRevisits**([*origCdxRdd*]) | Resolves the *revisit records* in the this collection of metadata records by setting the correct location and actual type of the corresponding (W)ARC record from a given set of original CDX records. If no such a set is given, the original records are looked up in the current dataset.  This operation can only be applied to metadata-only collection, loaded via [`CdxHdfsSpec`](DataSpecs.md).
&nbsp; | *Example:* `val resolved = cdxRdd.resolveRevisits()`
**saveAsWarc**(*path*, *info*, [*generateCdx*]) | Saves Web archive records in your dataset as WARC(.gz) format. Additional meta information to be included in the resulting WARC field can be specified as a [`WarcMeta`](../src/main/scala/de/l3s/archivespark/specific/warc/WarcMeta.scala) object. By default, it generates CDX files for the WARC records under the same path, this can be turned of by the third parameter. If the path ends in a `.gz` extension, the resulting output will automatically be compressed using GZip with each record compressed individually.
&nbsp; | *Example:* `rdd.saveAsWarc("/path/to/warc.gz", WarcMeta(publisher = "me"), generateCdx = true)`
**toCdxStrings** | Converts the Web archive records in the dataset to a new RDD consisting of their meta data in CDX format (in the common form used by the Internet Archive's CDX server and many other tools). 
&nbsp; | *Example:* `val cdx = rdd.toCdxStrings`
**saveAsCdx**(*path*) | Saves the meta data of the Web archive records in the dataset as their CDX representations. If the path ends in a `.gz` extension, the resulting output will automatically be compressed using GZip.
&nbsp; | *Example:* `rdd.saveAsCdx("/path/to/cdx.gz")`

## Record Operations

The following methods are defined on individual ArchiveSpark records instead of the datasets / RDDs.

Operation| Description
:--------|:---
**getValue**(*field*) | Gets the value stored in the specified field. If the field is specified by an Enrich Function, the records in the dataset first need to be enriched with it (s. `rdd.enrich(...)`). This operation will fail and throw an exception if the given field does not exist.
&nbsp; | *Example:* `val titles = enriched.map(_.getValue(Html.first("title")))`
**value**(*field*) | Gets an `Option` for the value stored in the specified field. The option will be defined with the value of the field it is exists. Hence, this can also be used to filter whether a field exists or return a default value if not, using Scala's `Option#getOrElse`.
&nbsp; | *Example:* `val filtered = rdd.filter(_.value(Html.first("title")).isDefined)`
**valueOrElse**(*field*, *else*) | Gets the value stored in the specified field if the field exists or a default value else.
&nbsp; | *Example:* `val titles = rdd.map(_.valueOrElse(Html.first("title"), "no title"))`

## Operations on Enrich Functions

The following operations on Enrich Functions create a new Enrich Function, either by changing the dependency or mapping the resulting value by applying some transformation as shown in the examples below.

Operation| Description
:--------|:---
**on**(*source*) / **of**(*source*) | Changes the dependency of the Enrich Function to the specified source, which can either be another Enrich Function or a source field in dot notation.
&nbsp; | *Example:* `val Tile = HtmlText.of(Html.first("title"))`
**onEach**(*source*) / **ofEach**(*source*) | Changes the dependency of the Enrich Function to be applied to each value of the speficied source, in case the source consists of multiple values.
&nbsp; | *Example:* `val AnchorTexts = HtmlText.ofEach(Html.all("a"))`
**map**(*target*)(*func*) | Creates a new Enrich Function that is dependent on the current one, applying the logic defined by the given function or inline lambda expression. The input of the new function will be the value of output of this Enrich Function and the result will be stored in the *target* field under the output of this.
&nbsp; | *Example:* `val TitleLength = Title.map("length"){str: String => str.length}`
**mapEach**(*target*)(*func*) | Creates a new Enrich Function that is applied to each value of the current one, in case it produces multiple values. The result will be stored in the *target* field under the each value of this..
&nbsp; | *Example:* `val LowerCaseLinks = Html.all("a").mapEach("lower"){str: String => str.toLowerCase}`
**mapMulti**(*target*)(*func*) | Creates a new Enrich Function that is dependent on the current one, but produces multiple values instead of one. The resulting values will be stored as a sequence in the *target* field under the output of this.
&nbsp; | *Example:* `val TitleTerms = Title.mapMulti("terms"){str: String => str.split(" ")}`
**toMulti**([*field*], *target*) | Transforms a single-value Enrich Function into a muli-value Enrich Function to be applied on a multi-value field. It now can be combined with multi-value Enrich Functions by using `on/of` instead of `onEach/ofEach`, however, the lineage of which result is derived from which source value is lost by this operation. The target parameter specifies the name of the new target field name (usually the plural version of the original field name). If no field is specified, the default field  (if available) is multiplied by this operation.
&nbsp; | *Example:* `val AnchorTexts = HtmlText.toMulti("texts").on(Html.all("a"))`

[< Table of Contents](README.md) | [Data Specifications (DataSpecs) >](DataSpecs.md)
:---|---: