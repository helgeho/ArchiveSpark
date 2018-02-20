[< Table of Contents](README.md) | [ArchiveSpark Recipies >](Recipes.md)
:---|---:

# General Usage

Before you start working with ArchiveSpark, you will need to import the required packages:

```scala
import de.l3s.archivespark._
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.specific.warc.implicits._
import de.l3s.archivespark.specific.warc.specs._
import de.l3s.archivespark.specific.warc.enrichfunctions._
```

The first three here are the general ArchiveSpark imports that are almost always needed. The last three are specific to Web archive datasets and may not be required if you use ArchiveSpark with different kinds of collections. In that case, you might need different or additional imports to get access to different datasets or additional enrich functions (the corresponding JAR files should be put into your libraries folder, s. [Install ArchiveSpark with Jupyter](Install_Juyter.md)).

If you are new to ArchiveSpark, please first read [Use ArchiveSpark with Jupyter](Use_Jupyter.md).

To load a dataset with ArchiveSpark, you first need to pick the appropriate [Data Specification (DataSpec)](DataSpecs.md).
More about DataSpecs can be read [here](DataSpecs.md), including a list of already provided ones. The general syntax to load a dataset is:

```scala
val dataset = ArchiveSpark.load(DataSpec)
```

The following example shows how to load a Web archive dataset from CDX/(W)ARC files:

```scala
val cdxPath = "/data/ia/derivatives/de/cdx/*/*.gz"
val warcPath = "/data/ia/w/de"
val rdd = ArchiveSpark.load(WarcCdxHdfsSpec(cdxPath, warcPath))
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
More about Enrich Functions can be read [here](EnrichFuncs.md).
The general syntax for this is:

```scala
val enriched = dataset.enrich(EnrichFunc)
``` 

The following example shows how to enrich a Web archive dataset with the text found on the website:

```scala
val enriched = filtered.enrich(HtmlText)
```

More about the available operations can be read [here](Operations.md).

Instead of reading the entire text, you can also get the text only for certain elements of the page, e.g., the title.
To do so, ArchiveSpark provides the option to chain multiple Enrich Functions, using the methods `on`, `onEach` or its aliases `of`, `ofEach`.
The following example shows how to enrich the records in a dataset with the title text as well as all link/anchor texts on a webpage:

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

Fore more information on available DataSpecs, Enrich Functions as well as the operations provided by ArchiveSpark, please read the following API Docs:
* [ArchiveSpark Operations](Operations.md)
* [Data Specifications (DataSpecs)](DataSpecs.md)
* [Enrich Functions](EnrichFuncs.md)

[< Table of Contents](README.md) | [ArchiveSpark Recipies >](Recipes.md)
:---|---: