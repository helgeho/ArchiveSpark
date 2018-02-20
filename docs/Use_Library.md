[< Table of Contents](README.md) | [Configuration >](Config.md)
:---|---:

# Use ArchiveSpark as a Library

Besides the interactive use of ArchiveSpark as a data analysis / corpus building platform as described in [Use ArchiveSpark with Jupyter](Use_Jupyter.md), it can be used as an API to archival collection in your own software, where it can be integrated as a library.

## Dependency

The recommended way to include ArchiveSpark as a library in your project is through Maven. For this purpose, we have published ArchiveSpark to [Maven Central](https://search.maven.org/#search%7Cga%7C1%7Carchivespark).

To include it from Maven Central in your Scala SBT project, add the following line to your `build.sbt` file (please check for the latest version):
```
libraryDependencies += "com.github.helgeho" %% "archivespark" % "2.7.5"
```

In addition to that, there are releases with the plain JAR files available on GitHub: https://github.com/helgeho/ArchiveSpark/releases.

## Usage

The general usage of ArchiveSpark is described in this article: [General Usage](General_Usage.md)

These instructions require a *Spark Context* to exist, which is automatically available if you use it with Jupyter as described in [Use ArchiveSpark with Jupyter](Use_Jupyter.md). If you would like to use it as a library in your own project, this *Spark Context* needs to be created manually as follows:

```scala
val appName = "ArchiveSpark"
val master = "yarn-client"

val conf = new SparkConf().setAppName(appName).setMaster(master)
val sc = new SparkContext(conf)
```

More details about this can be found in the official Spark documentation: [Spark Programming Guide](https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html).

Fore more information on the use and available DataSpecs, Enrich Functions as well as the operations provided by ArchiveSpark, please read the following API Docs:
* [ArchiveSpark Operations](Operations.md)
* [Data Specifications (DataSpecs)](DataSpecs.md)
* [Enrich Functions](EnrichFuncs.md)

[< Table of Contents](README.md) | [Configuration >](Config.md)
:---|---: