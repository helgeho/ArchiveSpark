## ArchiveSpark

An Apache Spark framework that facilitates access to Web Archives, enables easy data extraction as well as derivation.

### Build

At the moment we do not provide binaries. In order to use ArchiveSpark, please compile it yourself using the SBT assembly plugin:

1. `sbt assemblyPackageDependency`
2. `sbt assembly`

These commands will create two JAR files under `target/scala-2.10`, one for ArchiveSpark and one for the required dependencies.
Please include these files in your project to use ArchiveSpark or add them to your JVM classpath.

### Usage

There are multiple ways to use ArchiveSpark, among others: you can create a Java/Scala project to write your jobs and run it using `spark-submit` or use it interactively with `spark-shell`.
For more information on how to use [Apache Spark](http://spark.apache.org), please read the [documentation](http://spark.apache.org/docs/1.5.2).
 
ArchiveSpark is also compatible with [Jupyter](http://jupyter.org). We recommend to use [Apache Toree](https://github.com/apache/incubator-toree) as kernel for ArchiveSpark with Jupyter.
Example Jupyter notebooks can be found under [`notebooks`](notebooks).

#### Getting started

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

#### Prerequisites

1. ArchiveSpark requires some implicit functions to be imported: `import de.l3s.archivespark.implicits._`
2. ArchiveSpark requires the `KryoSerializer` to serialize its objects.<br/>Please set it using the Spark parameter `--conf spark.serializer=org.apache.spark.serializer.KryoSerializer`

### Related projects

__[ArchivePig](https://github.com/helgeho/ArchivePig)__

The original implementation of the ArchiveSpark concept was built on [Apache Pig](https://pig.apache.org) instead of Spark.
The project was the inspiration for this one and can be found under [ArchivePig](https://github.com/helgeho/ArchivePig).
However, it is not actively being developed anymore, but can be used if you prefer Pig over Spark.

__[Web2Warc](https://github.com/helgeho/Web2Warc)__

If you do not have Web archive data available to use it with ArchiveSpark, easily create your own from any collection of websites with [Web2Warc](https://github.com/helgeho/Web2Warc).

### License

The MIT License (MIT)

Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)

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
