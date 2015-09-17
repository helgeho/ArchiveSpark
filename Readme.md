## ArchiveSpark

An Apache Spark framework that facilitates access to Web Archives, enables easy data extraction as well as derivation.

### Build

At the moment there are no binaries provided. In order to use ArchiveSpark, please compile it yourself using the SBT assembly plugin:

`sbt assembly`

### Usage

First of all, a SparkContext needs to be implicitely available.
If a SparkContext already exists, e.g., in a `sc` variable when using `spark-shell`, you will need to make it implicit:

```scala
implicit val sparkContext = sc
```

Otherwise, create your own SparkContext as follows:

```scala
val appName = "ArchiveSpark"
val master = "yarn-client"
 
val conf = new SparkConf().setAppName(appName).setMaster(master)
implicit val sc = new SparkContext(conf)
```

Now you can load your Archive RDD using one of the convenience methods provided by the `ArchiveSpark` object, for instance from CDX and (W)ARC files stored on HDFS:

```scala
val rdd = ArchiveSpark.hdfs("/cdx/path/*.cdx", "/path/to/warc/and/arc")
```

Before you apply any enrichments we recommend to apply as much filters as possible based on the meta data, like URL, time or mime type:

```scala
val filteredRdd = rdd.filter(r => r.surtUrl.startsWith("com,example")) // only websites from exapmle.com
```

Up to this point ArchiveSpark has not touched your (W)ARC files. In order to extract actual contents from your archive, you can apply so-called enrich functions. These may depend one on another and allow for further filters based on content features.

```scala
val enriched = filteredRdd.enrich(Response) // extracts the response information from (W)ARC, i.e., headers and payload
```

Finally, you can save your data in a pretty JSON format:

```scala
enriched.saveAsJson("out.json")
```

(ArchiveSpark requires some implicit functions to be imported: `import de.l3s.archivespark.implicits._`)

### License

The MIT License (MIT)

Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
