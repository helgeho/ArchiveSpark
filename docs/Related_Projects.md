[< Table of Contents](README.md) | [Installing ArchiveSpark with Jupyter >](Installing_Jupyter.md)
:---|---:

# Related projects

Around ArchiveSpark, a few related projects have emerged: 

* **[ArchiveSpark-docker](https://github.com/helgeho/ArchiveSpark-docker)**

A [Docker](https://www.docker.com/) image with pre-configured Jupyter and ArchiveSpark to run on a local machine, server or cluster. 

* **[ArchiveSpark-server](https://github.com/helgeho/ArchiveSpark-server)**

Server application that provides a Web service API for ArchiveSpark to be used by third-party applications to integrate temporal web archive data with a flexible, easy-to-use interface. 

* **[ArchiveSpark2Triples](https://github.com/helgeho/ArchiveSpark2Triples)**

This library provides tools to convert ArchiveSpark records from web archives to [RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework) triples in [*Notation3 (N3)*](https://en.wikipedia.org/wiki/Notation3) format.

* **[HadoopConcatGz](https://github.com/helgeho/HadoopConcatGz)**

A Splitable Hadoop InputFormat for Concatenated GZIP Files and *.(w)arc.gz, used by ArchiveSpark to load plain web archive data (WARC) without a metadata index. (not anymore, before 3.0)

* **[Web2Warc](https://github.com/helgeho/Web2Warc)**

If you do not have web archive data available to be used with ArchiveSpark, easily create your own from any collection of websites with [Web2Warc](https://github.com/helgeho/Web2Warc).

* **[ArchivePig](https://github.com/helgeho/ArchivePig)**

The original implementation of the ArchiveSpark concept was powered by [Apache Pig](https://pig.apache.org) instead of Spark.
The project was the inspiration for this one and can be found under [ArchivePig](https://github.com/helgeho/ArchivePig).
However, it is not actively being developed anymore, but can be used if you prefer Pig over Spark.

[< Table of Contents](README.md) | [Installing ArchiveSpark with Jupyter >](Installing_Jupyter.md)
:---|---: