## ArchiveSpark

An Apache Spark framework that facilitates access to Web Archives, enables easy data extraction as well as derivation.

ArchiveSpark has been accepted as a full paper at [JCDL 2016](http://www.jcdl2016.org). If you use ArchiveSpark in your work, please cite this paper:

*[Helge Holzmann, Vinay Goel, Avishek Anand. ArchiveSpark: Efficient Web Archive Access, Extraction and Derivation. In Proceedings of the Joint Conference on Digital Libraries, Newark, New Jersey, USA, 2016.](http://dl.acm.org/citation.cfm?id=2910902)*

More information can be found in the slides presented at the [WebSci`16 Hackathon](http://www.websci16.org/hackathon):

http://www.slideshare.net/HelgeHolzmann/archivespark-introduction-websci-2016-hackathon

### Usage

We recommend to use ArchiveSpark interactively in a [Jupyter](http://jupyter.org) notebook. To install Jupyter locally or on your Spark/Hadoop cluster, please follow the official [instructions](https://jupyter.readthedocs.io/en/latest/install.html). In order to use Jupyter with ArchiveSpark we provide a pre-packaged kernel, based on [Apache Toree](https://github.com/apache/incubator-toree): [http://L3S.de/~holzmann/archivespark-kernel.tar.gz](http://L3S.de/~holzmann/archivespark-kernel.tar.gz)

Alternatively, a Docker image [ibnesayeed/archivespark](https://hub.docker.com/r/ibnesayeed/archivespark/) is provided for quick getting started without the need of installation and configuration.

The kernel is only compatible with Spark 1.6.1. If you only have an older version installed, please download 1.6.1 from the [Spark download page](https://spark.apache.org/downloads.html) and unpack it to your local home directory. Once you have Jupyter as well as Spark available, please follow the instructions below to configure it for the use with ArchiveSpark:

1. Create a kernels directory if it does not exist yet: `mkdir -p ~/.ipython/kernels`
2. Unpack the ArchiveSpark/Toree to your kernels dir: `tar -zxf archivespark-kernel.tar.gz -C ~/.ipython/kernels`
3. Edit the kernel configuration file `~/.ipython/kernels/archivespark/kernel.json` to customize it according to your environment (e.g., `vim ~/.ipython/kernels/archivespark/kernel.json`):
 * replace `USERNAME` in line 5 after `"argv": [` with your local username
 * set `SPARK_HOME` to the path of your Spark 1.6.1 installation
 * change `HADOOP_CONF_DIR` and the specified `SPARK_OPTS` if needed
4. Now you are all set to run jupyter: `jupyter notebook`

The first cell in your notebook should include the required imports:
```scala
import de.l3s.archivespark._
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.nativescala.implicits._
import de.l3s.archivespark.enrich._
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.specific.warc.implicits._
import de.l3s.archivespark.specific.warc._
import de.l3s.archivespark.specific.warc.specs._
```

In the next cell you can load your Web archive collection and start working:
```scala
val rdd = ArchiveSpark.load(sc, WarcHdfsSpec("/cdx/path/*.cdx", "/path/to/warc/and/arc"))
```

Example Jupyter notebooks can be found under [`notebooks`](notebooks). To understand the full stack from functional programming with Scala, how to work with Spark and to get started with ArchiveSpark, we suggest to read our tutorial notebook, that was presented in the hands-on session at the [WebSci'16 Hackathon](http://www.websci16.org/hackathon): [Example notebook](https://github.com/helgeho/ArchiveSpark/blob/master/notebooks/WebSciHackathonHandsOn.ipynb#ArchiveSpark)

For some enrich functions, such as [Entities](https://github.com/helgeho/ArchiveSpark/blob/master/src/main/scala/de/l3s/archivespark/enrich/functions/Entities.scala) and [Goose](https://github.com/helgeho/ArchiveSpark/blob/master/src/main/scala/de/l3s/archivespark/enrich/functions/Goose.scala), additional libraries are required. Please simply put the needed JAR files into `~/.ipython/kernels/archivespark/lib`. For `Entities` you will need to put the [Stanford CoreNLP](http://stanfordnlp.github.io/CoreNLP/) lib and models, for `Goose` you can get the corresponding JAR file from [Maven](http://mvnrepository.com/artifact/com.gravity/goose) or build it from [sources](https://github.com/GravityLabs/goose).

Besides Jupyter, there are several ways to use ArchiveSpark. For instance, you can use it as a library / API to access Web archives in your Java/Scala Spark projects, submitted with `spark-submit` or run interactively using `spark-shell`.
For more information on how to use [Apache Spark](http://spark.apache.org), please read the [documentation](http://spark.apache.org/docs/1.6.1).

### Build

To build the ArchiveSpark JAR files from source use the SBT assembly plugin:

1. `sbt assemblyPackageDependency`
2. `sbt assembly`

These commands will create two JAR files under `target/scala-2.10`, one for ArchiveSpark and one for the required dependencies.
Please include these files in your project to use ArchiveSpark or add them to your JVM classpath.

#### Getting started

First of all, a SparkContext needs to be available. If a SparkContext has not been created yet, e.g., in a `sc` variable when using Jupter (see above) or `spark-shell`, you will need to create your own:

```scala
val appName = "ArchiveSpark"
val master = "yarn-client"

val conf = new SparkConf().setAppName(appName).setMaster(master)
val sc = new SparkContext(conf)
```

Now you can load your Archive RDD using one of the convenience methods provided by the `ArchiveSpark` object, for instance from CDX and (W)ARC files stored on HDFS:

```scala
val rdd = ArchiveSpark.load(sc, WarcHdfsSpec("/cdx/path/*.cdx", "/path/to/warc/and/arc"))
```

Before you apply any enrichments we recommend to apply as much filters as possible based on the meta data, like URL, time or mime type:

```scala
val filteredRdd = rdd.filter(r => r.surtUrl.startsWith("com,example")) // only websites from exapmle.com
```

Up to this point ArchiveSpark has not touched your (W)ARC files. In order to extract actual contents from your archive, you can apply so-called enrich functions. These may depend one on another and allow for further filters based on content features.

```scala
val enriched = filteredRdd.enrich(WarcPayload) // extracts the response information from (W)ARC, i.e., headers and payload
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

Copyright (c) 2015-2016 Helge Holzmann ([L3S](http://www.L3S.de)) and Vinay Goel ([Internet Archive](http://www.archive.org))

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
