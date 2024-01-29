[< Table of Contents](README.md) | [Data Specifications (DataSpecs) >](DataSpecs.md)
:---|---:

# ArchiveSpark Operations

ArchiveSpark extends the existing Spark [*transformations*](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations) and [*actions*](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions) with additional operations for datasets (RDDs), single records (an item in an RDD), as well as field values.

Since version 3.0, values in a dataset / record are addressed by so-called Field Pointers (see [`FieldPointer`](../src/main/scala/org/archive/archivespark/model/pointers/FieldPointer.scala)). All Enrichment Functions are automatically Field Pointers, pointing at their default result field, and thus, can be used as parameters in all methods that expect a field reference. Besides, Field Pointers can be created manually by specifying a type along with an absolute path (e.g., `FieldPointer[String]("payload.string")`) or relative to an existing pointer, e.g., `pointer.parent[String]`, `pointer.child[String]("text")`, `pointer.sibling[Map[String,String]]("headers")`, etc. By default all pointers are assumed to address single values. If, for instance due to `ofEach` dependencies, there are multiple values under a path, this must be specified explicitely, e.g., `val list = record.value(pointer.multi)`.

## Dataset Operations

These operations become available on `RDD` with enrichable ArchiveSpark records by the following import `import org.archive.webservices.archivespark._`

Operation| Description
:--------|:---
**enrich**(*func*) | Enriches all records in a dataset with the given Enrichment Function and returns a new RDD with the enriched records. In case the dependencies of that function do not exist yet, these are automatically created prior to the specified function, but their values are not included in the output. The resulting value will be appended in the internal tree-like structure of the records according to the functions dependency lineage, i.e., `StringContent.of(WarcPayload)` would result in a field called `string` which is nested under another field named `payload`, to be addressed in dot notation as `payload.string`. 
&nbsp; | *Example:* `val enriched = rdd.enrich(StringContent)`
**filterExists**(*fieldPointer*) | Filters the records in a dataset based on whether the given field exists, e.g., to filter out records for which a given Enrichment Function has not generated any values.
&nbsp; | *Example:* `val filtered = enriched.filterExists(SomeEnrichFunc)`
**filterValue**(*fieldPointer*)(*filter*) | Filters the records in a dataset based on the value of the given field. The filter function gets the field value as an option, which is undefined if the field does not exist.
&nbsp; | *Example:* `val filtered = enriched.filterValue(SomeEnrichFunc)(opt => opt.isDefined && opt.get == someValue)`
**filterNonEmpty**(*fieldPointer*) | Filters the records in the dataset based on whether their value is non-empty, i.e., whether or not the field exists and the value in this field has a non-empty value. This can be applied to any datatype that defines a `nonEmpty` method, e.g., a string can be non-empty if it is longer than 0 characters or an `Option` would be non-empty if it holds a value.
&nbsp; | *Example:* `val filtered = enriched.filterNonEmpty(SomeEnrichFunc)`
**filterNoException**() | Filters the records in a dataset based on whether or not an exception was thrown for that record at the last enrichment by the applied Enrichment Function. Every call of `enrich` clears the last catched exception for each record. To use this operation, [`catchExceptions`](Config.md) has to be enabled (default).
&nbsp; | *Example:* `val filtered = enriched.filterNoException()`
**printLastException()** | Prints the last exception thrown by an Enrichment Function along with its stack trace. To use this operation, [`catchExceptions`](Config.md) has to be enabled (default). It prints the first exception found for a record in a dataset. Each call of `enrich` clears the last catched exception for each record.
&nbsp; | *Example:* `val ex = enriched.printLastException()`
**distinctValue**(*fieldPointer*)(*reduce*) | Returns a new RDD that consists only of records with distinct values in the given field. The given reduce function defines which of two records two keep in case they share the same value in the specified field. As an alternative to a Field Pointer, this method also accepts a mapping of the root record.
&nbsp; | *Example:* `val distinct = enriched.distinctValue(_.surtUrl) {(r1, r2) => Seq(r1, r2).maxBy(_.time)}`
**mapValues**(*fieldPointer*) | Transforms the records in a dataset to the values in the given field. If the field is specified by an Enrichment Function that has not been applied yet, it will automatically be initialized first.
&nbsp; | *Example:* `val titles = rdd.mapValues(Html.first("title"))`
**flatMapValues**(*source*) | Transforms the records in the datset to the values in the given field and flattens it, i.e., if the field holds multiple values, these values become the records of the resulting dataset. If the field is specified by an Enrichment Function that has not been applied yet, it will automatically be initialized first.
&nbsp; | *Example:* `val links = rdd.flatMapValues(Html.all("a"))`
**peekJson** | Returns the first record of the dataset as JSON. The tree-like structure of an ArchiveSpark record is reflected by nested JSON objects in this representation. This is useful to get an idea of what fields the records in your dataset consist of. If you use Jupyter to instruct ArchiveSpark interactively, you need to wrap it into a `print` in order to see the full output, in case the content of the record is too long.
&nbsp; | *Example:* `print(enriched.peekJson)`
**saveAsJson**(*path*) | Saves the records of a dataset as their corresponding JSON representation. This is the same output as you get by calling `peekJson` on an RDD. If the path ends in a `.gz` extension, the resulting output will automatically be compressed using GZip.
&nbsp; | *Example:* `enriched.saveAsJson("enriched.json.gz")`

### Web Archive-specific Dataset Operations

These operations are specific to web archive datasets and become available through `import org.archive.webservices.archivespark.specific.warc._`

Operation| Description
:--------|:---
**saveAsWarc**(*path*, [*info*], [*generateCdx*]) | Saves web archive records in your dataset as WARC(.gz) files. Additional meta information to be included in the resulting WARC records can be specified as a [`WarcFileMeta`](../src/main/scala/org/archive/archivespark/specific/warc/WarcFileMeta.scala) object. By default, it generates CDX files for the WARC records under the same path, this can be turned off by the third parameter. If the path ends in a `.gz` extension, the resulting output will automatically be compressed using GZip with each record compressed individually.
&nbsp; | *Example:* `rdd.saveAsWarc("/path/to/warc.gz", WarcFileMeta(publisher = "me"), generateCdx = true)`
**toCdxStrings** | Converts the web archive records in a dataset to a new RDD consisting of their meta data in CDX format (in the common form used by the Internet Archive's CDX server and many other tools). 
&nbsp; | *Example:* `val cdx = rdd.toCdxStrings`
**saveAsCdx**(*path*) | Saves the meta data of the web archive records in a dataset as CDX. If the path ends in a `.gz` extension, the resulting output will automatically be compressed using GZip.
&nbsp; | *Example:* `rdd.saveAsCdx("/path/to/cdx.gz")`

## Record Operations

The following methods are defined on individual ArchiveSpark records in a dataset / RDD.

Operation| Description
:--------|:---
**getValue**(*fieldPointer*) | Gets the value stored in the specified field. If the field is specified by an Enrichment Function that has not been applied yet, it will automatically be initialized first.
&nbsp; | *Example:* `val titles = rdd.map(_.getValue(Html.first("title")))`
**value**(*fieldPointer*) | Gets an `Option` for the value stored in the specified field. The option will be defined with the value of the field it is exists or undefined if not. If the field is specified by an Enrichment Function that has not been applied yet, it will automatically be initialized first.
&nbsp; | *Example:* `val filtered = rdd.filter(_.value(Html.first("title")).isDefined)`
**valueOrElse**(*fieldPointer*, *else*) | Gets the value stored in the specified field if the field exists or a default value else.  If the field is specified by an Enrichment Function that has not been applied yet, it will automatically be initialized first.
&nbsp; | *Example:* `val titles = rdd.map(_.valueOrElse(Html.first("title"), "no title"))`

## Operations on Field Values / Enrichment Functions

The following operations are defined on fields specified by Field Pointers / Enrichment Functions to create a new Enrichment Function, either by changing the dependency or mapping the resulting value by applying a transformation as shown in the examples below.

Operation| Description
:--------|:---
**on**(*fieldPointer*) / **of**(*fieldPointer*) | Changes the dependency of an Enrichment Function to the specified source field.
&nbsp; | *Example:* `val Tile = HtmlText.of(Html.first("title"))`
**onEach**(*fieldPointer*) / **ofEach**(*fieldPointer*) | Changes the dependency of an Enrichment Function to each value of the specified multi-value source field. The resulting Enrichment Function represents single result value to be mapped / derived further. The access the list of multiple values, `.mutli` must be called on it.
&nbsp; | *Example:* `val AnchorTexts = HtmlText.ofEach(Html.all("a"))`
**map**(*target*)(*func*) | Creates a new Enrichment Function that dependents on the current field / derives from it with the logic defined by the given function or inline lambda expression. The result will be stored in a field named *target* under the parent field.
&nbsp; | *Example:* `val TitleLength = Title.map("length")(_.length)`
**mapMulti**(*target*)(*func*) | Creates a new Enrichment Function that dependents on the current field / derives from it with the logic defined by the given function or inline lambda expression. The result if must be a multi-value list, which will be stored in a field named *target* under the parent field, to be derived further using `onEach` / `ofEach` or `.each.map(single => ...)`.
&nbsp; | *Example:* `val TitleTerms = Title.mapMulti("terms")(_.split(" "))`

[< Table of Contents](README.md) | [Data Specifications (DataSpecs) >](DataSpecs.md)
:---|---: