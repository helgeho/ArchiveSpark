# ArchiveSpark Documentation

ArchiveSpark is a Java/JVM library, written in Scala, based on [Apache Spark](https://spark.apache.org), which can be used as a library in any Java/Scala/JVM program as an API for easy and efficient access to Web archives and other supported datasets. In addition to that, it can be used stand-alone using Scala's interactive shell or notebook tools, such as Jupyter.

To get familiar with ArchiveSpark, but also for most of the common use cases, we recommend the use with Jupyter. In order to get you started more easily, we provide a pre-packaged and pre-configured [Docker](https://www.docker.com/) container with ArchiveSpark and Jupyter ready to run, just one command away: https://github.com/helgeho/ArchiveSpark-docker

## Getting Started
* [Install ArchiveSpark with Jupyter](Install_Jupyter.md)
* [Use ArchiveSpark with Jupyter](Use_Jupyter.md)
* [General Usage](General_Usage.md)
* [ArchiveSpark Recipies](Recipes.md)
* [Build ArchiveSpark](Build.md) (advanced) 
* [Use ArchiveSpark as a Library](Use_Library.md) (advanced)

## API Docs
* [Configuration](Config.md)
* [ArchiveSpark Operations](Operations.md)
* [Data Specifications (DataSpecs)](DataSpecs.md)
* [Enrich Functions](EnrichFuncs.md)

## Developer Documentation
* [Contribute](Contribute.md)
* [How to Implement DataSpecs](Dev_DataSpecs.md)
* [How to Implement Enrich Functions](Dev_EnrichFuncs.md)

## Cite

ArchiveSpark is described and published in two research papers, which you should cite when you use ArchiveSpark in your own work:

* The first and main paper was the presentation of ArchiveSpark at JCDL 2016 (Best Paper Nominee). It describes the core ideas and includes benchmarks:
  * [H. Holzmann, V. Goel and A. Anand. *ArchiveSpark: Efficient Web Archive Access, Extraction and Derivation.* 16th ACM/IEEE-CS Joint Conference on Digital Libraries (JCDL). Newark, New Jersey, USA. June 2016.](http://dl.acm.org/citation.cfm?id=2910902) [**Get full-text PDF**](http://www.helgeholzmann.de/papers/JCDL_2016_ArchiveSpark.pdf)
* We later presented the extensions to ArchiveSpark to make it a more universal / generic data processing platform for any archival collection at IEEE BigData 2017 (Short Paper):
  * [H. Holzmann, Emily Novak Gustainis and Vinay Goel. *Universal Distant Reading through Metadata Proxies with ArchiveSpark*. 5th IEEE International Conference on Big Data (BigData). Boston, MA, USA. December 2017.](http://cci.drexel.edu/bigdata/bigdata2017/AcceptedPapers.html) [**Get full-text PDF**](http://www.helgeholzmann.de/papers/BIGDATA_2017.pdf)