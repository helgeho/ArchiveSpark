[< Table of Contents](README.md) | [ArchiveSpark Recipies >](Recipes.md)
:---|---:

# General Usage

Before you start working with ArchiveSpark, you will need to import a few required packages:

```scala
import org.archive.webservices.archivespark._
import org.archive.webservices.archivespark.functions._
import org.archive.webservices.archivespark.specific.warc._
```

The first two of these are the general ArchiveSpark imports that are almost always needed. The last one is specific to web archive datasets and may not be required if you use ArchiveSpark with different kinds of collections. In that case, you might need different or additional imports to get access to different datasets or additional enrichment functions (the corresponding JAR files should be put into your libraries folder, s. [Install ArchiveSpark with Jupyter](Install_Juyter.md)).

If you are new to ArchiveSpark, please first read [Use ArchiveSpark with Jupyter](Using_Jupyter.md).

To load a dataset with ArchiveSpark, you first need to pick the appropriate [Data Specification (DataSpec)](DataSpecs.md).
More about DataSpecs can be read [here](DataSpecs.md), including a list of already provided ones. The general syntax to load a dataset is:

```scala
val dataset = ArchiveSpark.load(DataSpec)
```

The following example shows how to load a web archive dataset from CDX/(W)ARC files:

```scala
val cdxPath = "/data/cdx/*/*.cdx.gz"
val warcPath = "/data/warc"
val rdd = ArchiveSpark.load(WarcSpec.fromFiles(cdxPath, warcPath))
```

At any time, you can have a quick look at a pretty-printed JSON representation of the first record in your dataset (*here:* `rdd`) to check the current state and the result of your last operations:

```scala
rdd.peekJson
```

Before you continue, you should filter your dataset based on the corresponding metadata information, e.g.:

```scala
val filtered = rdd.filter(r => r.status == 200 && r.mime == "text/html")
``` 

This would only keep those records in the dataset that have a status code of 200 (success) and a mime type of *text/html* (webpages).

Now you can apply various enrichments onto you dataset.
More about Enrichment Functions can be read [here](EnrichFuncs.md).
The general syntax for this is:

```scala
val enriched = dataset.enrich(EnrichFunc)
``` 

The following example shows how to enrich a web archive dataset with the text on the webpages:

```scala
val enriched = filtered.enrich(HtmlText)
```

More about the available operations can be read [here](Operations.md).

Instead of reading the entire text, you can also get the text only for certain elements of the page, e.g., the title.
To do so, ArchiveSpark provides the option to chain multiple Enrichment Functions, using the methods `on`, `onEach` or its aliases `of`, `ofEach`.
The following example shows how to enrich the records in a dataset with the title text as well as all links / anchor texts on a webpage:

```scala
val TitleText = HtmlText.of(Html.first("title"))
val LinkText = HtmlText.ofEach(Html.all("a"))
val enriched = filtered.enrich(TitleText).enrich(LinkText)
```

Finally, you can save your enriched dataset as pretty-printed JSON object, which can be loaded and post-processed as part of your remaining workflow.
The lineage reflected by these JSON objects together with this script / the corresponding Jupyter notebook, constitutes a complete description of your resulting corpus:

```scala
enriched.saveAsJson("result.json.gz")
```

Fore more information on available DataSpecs, Enrichment Functions as well as the operations provided by ArchiveSpark, please read the following API Docs:
* [ArchiveSpark Operations](Operations.md)
* [Data Specifications (DataSpecs)](DataSpecs.md)
* [Enrichment Functions](EnrichFuncs.md)

[< Table of Contents](README.md) | [ArchiveSpark Recipies >](Recipes.md)
:---|---: